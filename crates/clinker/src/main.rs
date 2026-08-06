use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};
use clinker_core_types::FailureClassification;
use serde::Serialize;

use clinker_exec::executor::PipelineExecutor;
use clinker_exec::metrics::{self, ExecutionMetrics};
use clinker_plan::config::utils::parse_memory_limit_bytes_strict;
use clinker_plan::error::PipelineError;

mod machine;
mod refactor;

use machine::MachineEmitter;

/// Bounded-memory batch ETL engine for CXL pipelines.
#[derive(Parser, Debug)]
#[command(
    name = "clinker",
    version,
    about = "Bounded-memory batch ETL engine for CXL pipelines",
    long_about = "\
Clinker is a bounded-memory, single-process batch ETL engine. It reads finite \
tabular inputs (CSV, NDJSON), applies CXL transformation expressions to each \
record, and writes the results to output files. A run is a finite job: sources \
read until EOF, the DAG drains, and the process exits — it is not a \
long-running stream processor. Records are evaluated one at a time within that \
bounded run, so memory stays capped regardless of input size.\n\n\
Pipelines are defined in YAML configuration files that specify inputs, outputs, \
field mappings with CXL expressions, and optional channel overrides for \
multi-tenant customization.",
    after_long_help = "\
QUICK START:
  clinker run pipeline.yaml
  clinker run pipeline.yaml --dry-run -n 10
  clinker run pipeline.yaml --explain
  clinker run pipeline.yaml --channel acme-corp
  clinker attempts list pipeline.yaml

ENVIRONMENT VARIABLES:
  CLINKER_ENV                   Active environment for when: conditions
  CLINKER_METRICS_SPOOL_DIR     Default metrics spool directory

EXIT CODES:
  0  Success
  1  Configuration, schema, or CXL compilation error
  2  Pipeline completed but DLQ entries were produced
  3  CXL evaluation error
  4  Infrastructure failure or retained-attempt cleanup debt"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Commands {
    /// Run a pipeline from a YAML config file
    #[command(
        long_about = "\
Run a pipeline from a YAML configuration file. The pipeline reads input \
files, applies CXL transformation expressions to each record, and writes \
results to the configured outputs. Records that fail evaluation are routed \
to a dead-letter queue (DLQ).",
        after_long_help = "\
EXAMPLES:
  # Run a pipeline
  clinker run pipeline.yaml

  # Preview the execution plan without reading data
  clinker run pipeline.yaml --explain

  # Validate config and process 10 records as a dry run
  clinker run pipeline.yaml --dry-run -n 10

  # Run with a channel override for multi-tenant customization
  clinker run pipeline.yaml --channel acme-corp

  # Run with custom memory budget and thread count
  clinker run pipeline.yaml --memory-limit 512M --threads 4

  # Spool execution metrics for later collection
  clinker run pipeline.yaml --metrics-spool-dir /var/spool/clinker"
    )]
    Run(RunArgs),
    /// Metrics utilities
    #[command(long_about = "\
Utilities for collecting and managing pipeline execution metrics. Clinker \
can spool per-execution metrics as JSON files during pipeline runs. Use \
these subcommands to sweep spool directories and consolidate metrics into \
NDJSON archives.")]
    Metrics {
        #[command(subcommand)]
        subcommand: MetricsCommands,
    },
    /// Inspect and purge retained publication attempts.
    #[command(
        long_about = "\
Inspect retained publication attempts owned by a freshly compiled pipeline. \
List and inspect never mutate attempt state. Purge is a preview unless \
--execute is supplied, and every operation remains bounded by \
[storage.publication].",
        after_long_help = "\
EXAMPLES:
  clinker attempts list pipelines/orders.yaml
  clinker attempts inspect pipelines/orders.yaml --execution-id 018f47a2-9a41-7a27-b4d6-4f7137e3c159
  clinker attempts purge pipelines/orders.yaml --expired
  clinker attempts purge pipelines/orders.yaml --expired --execute"
    )]
    Attempts {
        #[command(subcommand)]
        subcommand: AttemptsCommands,
    },
    /// Explain pipeline field provenance or error codes
    #[command(
        long_about = "\
Inspect field-level provenance chains or look up error/warning code documentation.\n\n\
Use --field to trace where a resolved value comes from across all configuration \
layers. An exact `/v1/config/...` or `/v1/schema/...` address is unambiguous. \
A two-part `node.param` shorthand traces a composition config value when the \
authored name is unique \
(composition defaults, channel defaults, channel fixed); a three-part \
`source.column.attribute` path traces a source-schema attribute across the \
Base < Pipeline < Group < Channel schema layers. \
Use --code to look up the documentation for a diagnostic code (composition codes \
E101–E108, combine codes E300-E319/E325/E326/E327 and W302/W305/W306, memory codes E310-E312, \
spill codes E320/E321, EDI output-split codes E323/E338, storage-validation \
codes E330-E334, staging-copy codes E335-E337, the multi-record discriminator \
code E345, and W101).",
        after_long_help = "\
EXAMPLES:
  # Show provenance for a composition config field
  clinker explain pipeline.yaml --field enrich1.fuzzy_threshold

  # Show provenance with a channel overlay applied
  clinker explain pipeline.yaml --field enrich1.fuzzy_threshold --channel acme_prod

  # Trace a source-schema attribute across the Base/Pipeline/Group/Channel layers
  clinker explain pipeline.yaml --field orders.amount.scale --channel acme_prod

  # Look up error code documentation
  clinker explain --code E103"
    )]
    Explain(ExplainArgs),
    /// Channel/group overlay tooling: resolve one effective plan, or lint the
    /// whole workspace.
    #[command(
        long_about = "\
Inspect and validate the channel/group multi-tenant overlay system.\n\n\
`resolve` renders the effective post-overlay DAG for one target under a chosen \
channel and/or groups, with per-value provenance — which layer supplied each \
value and which group injected which node. `lint` compiles every \
(target × overlay) combination across the workspace and reports failures, the \
CI safety net for base-change blast radius.",
        after_long_help = "\
EXAMPLES:
  # What does tenant `globex` actually run for this pipeline?
  clinker channels resolve pipeline/order_fulfillment.yaml --channel globex

  # Preview a group overlay standalone (no channel)
  clinker channels resolve pipeline/order_fulfillment.yaml --group enterprise

  # Compile every channel/group overlay in the workspace and report failures
  clinker channels lint"
    )]
    Channels {
        #[command(subcommand)]
        subcommand: ChannelsCommands,
    },
    /// Workspace-wide refactors over pipelines and their channel/group overlays.
    #[command(
        long_about = "\
Structural refactors that span a base pipeline and every channel/group overlay \
that references it.\n\n\
`rename-node` renames a base node and propagates the rename to every overlay \
reference — op `target`, splice anchors, `rewire` paths, injected `alias`, \
`config` dotted-paths, and CXL input-alias references in Combine `where:`/`cxl:` \
bodies — with a `--dry-run` preview and a `channels lint` re-check afterward.",
        after_long_help = "\
EXAMPLES:
  # Preview a rename across the base pipeline and every overlay
  clinker refactor rename-node pipeline/order_fulfillment.yaml orders purchases --dry-run

  # Apply it, then re-lint the workspace
  clinker refactor rename-node pipeline/order_fulfillment.yaml orders purchases"
    )]
    Refactor {
        #[command(subcommand)]
        subcommand: RefactorCommands,
    },
    /// Inspect and canonicalize a pipeline config file.
    #[command(
        long_about = "\
Inspect a pipeline configuration file.\n\n\
`--resolved` prints the config with the multi-value shorthand expanded to its \
canonical, fully-materialized form: the bare-field forms of `split_to_rows:`, \
`split_values:`, and `join_values:` are rewritten to full mappings with every \
default spelled out (`keep_empty`, `mode`, `delimiter`, `on_conflict`, …). The \
rewrite is surgical — only those shorthand blocks change; comments, key order, \
indentation, and every other surface are preserved byte-for-byte, so the output \
parses to the same plan and re-resolving it is a no-op. This is config \
canonicalization, distinct from `channels resolve`, which renders the effective \
post-overlay plan for a tenant.",
        after_long_help = "\
EXAMPLES:
  # Show the fully-expanded canonical form of a pipeline's shorthand
  clinker config --resolved pipeline.yaml

  # Overwrite a file in place with its canonical form
  clinker config --resolved pipeline.yaml > pipeline.canonical.yaml"
    )]
    Config(ConfigArgs),
}

/// Subcommands for `clinker attempts`.
#[derive(Subcommand, Debug)]
pub enum AttemptsCommands {
    /// List retained attempts owned by the compiled pipeline.
    List(AttemptsListArgs),
    /// Inspect one execution across every owned destination root.
    Inspect(AttemptsInspectArgs),
    /// Preview or execute bounded metadata-last cleanup.
    Purge(AttemptsPurgeArgs),
}

/// Output format for retained-attempt operations.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AttemptsFormat {
    /// Deterministic human-readable records.
    Text,
    /// Compact JSON for tooling consumption.
    Json,
}

/// Arguments for `clinker attempts list`.
#[derive(Parser, Debug)]
pub struct AttemptsListArgs {
    /// Workspace-relative pipeline YAML used to derive owned roots.
    pub pipeline: PathBuf,
    #[command(flatten)]
    pub identity: AttemptIdentityArgs,
    /// Resume one bounded page using the exact opaque token previously emitted.
    #[arg(long)]
    pub continuation: Option<String>,
    /// Include sanitized workspace-relative attempt paths.
    #[arg(long)]
    pub show_paths: bool,
    /// Render deterministic text or compact JSON.
    #[arg(long, value_enum, default_value_t = AttemptsFormat::Text)]
    pub format: AttemptsFormat,
}

/// Arguments for `clinker attempts inspect`.
#[derive(Parser, Debug)]
pub struct AttemptsInspectArgs {
    /// Workspace-relative pipeline YAML used to derive owned roots.
    pub pipeline: PathBuf,
    /// Canonical execution identity to inspect.
    #[arg(long)]
    pub execution_id: String,
    #[command(flatten)]
    pub identity: AttemptIdentityArgs,
    /// Include sanitized workspace-relative attempt paths.
    #[arg(long)]
    pub show_paths: bool,
    /// Render deterministic text or compact JSON.
    #[arg(long, value_enum, default_value_t = AttemptsFormat::Text)]
    pub format: AttemptsFormat,
}

/// Arguments for `clinker attempts purge`.
#[derive(Parser, Debug)]
pub struct AttemptsPurgeArgs {
    /// Workspace-relative pipeline YAML used to derive owned roots.
    pub pipeline: PathBuf,
    /// Purge one canonical execution identity.
    #[arg(long)]
    pub execution_id: Option<String>,
    #[command(flatten)]
    pub identity: AttemptIdentityArgs,
    /// Purge every policy-expired attempt admitted by this bounded page.
    #[arg(long)]
    pub expired: bool,
    /// Perform cleanup. Without this flag, purge is always a preview.
    #[arg(long)]
    pub execute: bool,
    /// Resume one bounded page using the exact opaque token previously emitted.
    #[arg(long)]
    pub continuation: Option<String>,
    /// Include sanitized workspace-relative attempt paths in preview evidence.
    #[arg(long)]
    pub show_paths: bool,
    /// Render deterministic text or compact JSON.
    #[arg(long, value_enum, default_value_t = AttemptsFormat::Text)]
    pub format: AttemptsFormat,
}

/// Run identity inputs needed to recompile the exact set of owned attempt
/// roots. These mirror the path- and overlay-affecting `run` options; no raw
/// destination path is accepted as cleanup authority.
#[derive(Args, Debug)]
pub struct AttemptIdentityArgs {
    /// Workspace root used by the original run.
    #[arg(long, help_heading = "Paths")]
    pub base_dir: Option<PathBuf>,
    /// Permit absolute paths in YAML config, matching `clinker run`.
    #[arg(long, help_heading = "Paths")]
    pub allow_absolute_paths: bool,
    /// CXL module search path used by the original run.
    #[arg(long, help_heading = "Paths")]
    pub rules_path: Option<PathBuf>,
    /// Channel overlay applied by the original run.
    #[arg(long, help_heading = "Configuration")]
    pub channel: Option<String>,
    /// Explicit group overlay applied by the original run (repeatable).
    #[arg(long = "group", value_name = "NAME", help_heading = "Configuration")]
    pub groups: Vec<String>,
    /// Suppress selector-derived groups, matching `clinker run`.
    #[arg(long = "no-auto-groups", help_heading = "Configuration")]
    pub no_auto_groups: bool,
    /// Execution identity used only to reconstruct run-scoped output paths.
    /// This is distinct from an inspect or purge selector.
    #[arg(long, help_heading = "Configuration")]
    pub path_execution_id: Option<String>,
    /// Batch identity used by an output path template in the original run.
    #[arg(long, help_heading = "Configuration")]
    pub batch_id: Option<String>,
    /// Timestamp token used by an output path template in the original run.
    #[arg(
        long,
        value_name = "YYYY-MM-DDTHH-MM-SSZ",
        help_heading = "Configuration"
    )]
    pub timestamp: Option<String>,
}

/// Subcommands for `clinker channels`.
#[derive(Subcommand, Debug)]
pub enum ChannelsCommands {
    /// Render the effective post-overlay plan for one target with provenance.
    Resolve(ResolveArgs),
    /// Compile every (target × overlay) combination and report failures.
    Lint(LintArgs),
    /// Group membership queries (which channels a group's selector matches).
    Group {
        #[command(subcommand)]
        subcommand: GroupCommands,
    },
    /// Bulk channel-label editing.
    Label {
        #[command(subcommand)]
        subcommand: LabelCommands,
    },
}

/// Subcommands for `clinker channels group`.
#[derive(Subcommand, Debug)]
pub enum GroupCommands {
    /// List the channels a group's selector currently matches.
    Members(GroupMembersArgs),
}

/// Subcommands for `clinker channels label`.
#[derive(Subcommand, Debug)]
pub enum LabelCommands {
    /// Stamp/overwrite a label across the named channels (idempotent).
    Set(LabelSetArgs),
}

/// Subcommands for `clinker refactor`.
#[derive(Subcommand, Debug)]
pub enum RefactorCommands {
    /// Rename a base node and propagate the rename to every overlay reference.
    RenameNode(RenameNodeArgs),
}

/// Arguments for `clinker config`.
#[derive(Parser, Debug)]
pub struct ConfigArgs {
    /// Path to the pipeline YAML configuration file.
    pub config: PathBuf,

    /// Print the config with the multi-value shorthand (`split_to_rows`,
    /// `split_values`, `join_values`) expanded to canonical full-mapping form
    /// with every default materialized. Everything outside those blocks is
    /// preserved byte-for-byte.
    #[arg(long)]
    pub resolved: bool,
}

/// Arguments for `clinker channels group members`.
#[derive(Parser, Debug)]
pub struct GroupMembersArgs {
    /// Group name (the `group.name` of a `*.group.yaml`).
    pub group: String,

    /// Workspace root (holds `clinker.toml` and the channel/group roots).
    #[arg(long, default_value = ".")]
    pub base_dir: PathBuf,
}

/// Arguments for `clinker channels label set`.
#[derive(Parser, Debug)]
pub struct LabelSetArgs {
    /// Label assignment as `key=value`. The value is typed by YAML scalar
    /// inference (`true`/`false` → bool, integers → int, decimals → float,
    /// everything else → string) so numeric/boolean labels match selectors.
    #[arg(value_name = "KEY=VALUE")]
    pub assignment: String,

    /// One or more channel ids (tenant folder names) to stamp the label on.
    #[arg(required = true, value_name = "CHANNEL_ID")]
    pub ids: Vec<String>,

    /// Workspace root (holds `clinker.toml` and the channel root).
    #[arg(long, default_value = ".")]
    pub base_dir: PathBuf,
}

/// Arguments for `clinker refactor rename-node`.
#[derive(Parser, Debug)]
pub struct RenameNodeArgs {
    /// Path to the base pipeline (or composition) YAML that declares the node.
    pub target: PathBuf,

    /// Current node name.
    pub old: String,

    /// New node name (letters, digits, and `_` only).
    pub new: String,

    /// Print the diff of every file that would change without writing anything.
    #[arg(long)]
    pub dry_run: bool,

    /// Workspace root (holds `clinker.toml` and the channel/group roots).
    #[arg(long, default_value = ".")]
    pub base_dir: PathBuf,
}

/// Arguments for `clinker channels resolve`.
#[derive(Parser, Debug)]
pub struct ResolveArgs {
    /// Path to the base pipeline (or composition) YAML to resolve.
    pub target: PathBuf,

    /// Channel id to resolve the overlay stack for (folder under the channel
    /// root). Derives matching groups from the channel's labels.
    #[arg(long)]
    pub channel: Option<String>,

    /// Force-include a group overlay by name (repeatable), with or without a
    /// channel.
    #[arg(long = "group", value_name = "NAME")]
    pub groups: Vec<String>,

    /// Suppress selector-derived group membership; only explicit `--group`
    /// overlays apply.
    #[arg(long = "no-auto-groups")]
    pub no_auto_groups: bool,

    /// Workspace root (holds `clinker.toml` and the channel/group roots).
    #[arg(long, default_value = ".")]
    pub base_dir: PathBuf,
}

/// Arguments for `clinker channels lint`.
#[derive(Parser, Debug)]
pub struct LintArgs {
    /// Workspace root to lint (holds `clinker.toml` and the channel/group
    /// roots).
    #[arg(long, default_value = ".")]
    pub base_dir: PathBuf,
}

/// Output format for --explain.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ExplainFormat {
    /// Human-readable ASCII text with branch/merge indicators.
    Text,
    /// Structured JSON for tooling consumption.
    Json,
    /// Graphviz DOT for static visualization.
    Dot,
}

/// Versioned machine-readable run protocol.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum MachineFormat {
    /// Compact UTF-8 NDJSON using `clinker.run` schema version 1.
    NdjsonV1,
}

/// Arguments for `clinker run`.
#[derive(Parser, Debug)]
pub struct RunArgs {
    /// Path to the pipeline YAML configuration file
    pub config: PathBuf,

    /// Memory budget (supports K/M/G suffixes), default 512M
    #[arg(long = "memory-limit", help_heading = "Execution")]
    pub mem_limit: Option<String>,

    /// Thread pool size, default num_cpus
    #[arg(long, help_heading = "Execution")]
    pub threads: Option<usize>,

    /// Max DLQ records before abort, 0 = unlimited
    #[arg(long, default_value = "0", help_heading = "Execution")]
    pub error_threshold: u64,

    /// Pipeline batch_id, default generated UUID v7
    #[arg(long, help_heading = "Execution")]
    pub batch_id: Option<String>,

    /// Emit one ordered machine lifecycle on stdout. Consumers must reject
    /// unsupported major schema versions; schema-1 additions are compatible.
    #[arg(long, value_enum, help_heading = "Output")]
    pub machine: Option<MachineFormat>,

    /// Print execution plan and exit (no data read).
    /// Optionally specify format: text (default), json, dot.
    #[arg(
        long,
        help_heading = "Validation",
        num_args(0..=1),
        default_missing_value("text"),
        value_enum
    )]
    pub explain: Option<ExplainFormat>,

    /// Build column lineage and write OpenLineage NDJSON, then exit (no data
    /// read). Give a file path, or `-` for stdout. A plan-only export, so it
    /// cannot be combined with --explain, --dry-run, or -n.
    #[arg(
        long,
        value_name = "PATH",
        help_heading = "Validation",
        conflicts_with_all = ["explain", "dry_run", "dry_run_n"]
    )]
    pub lineage: Option<PathBuf>,

    /// Run the pipeline and emit live OpenLineage run events (a START at run
    /// begin, then a terminal COMPLETE / FAIL / ABORT with real timing and row
    /// counts) as NDJSON to a file path, or `-` for stdout. Unlike --lineage
    /// (a static plan-only export that exits without reading data), this
    /// processes data, so it cannot be combined with --lineage, --explain,
    /// --dry-run, or -n. Prefer a file path for a clean NDJSON stream: with `-`,
    /// the run's own stdout output (e.g. the spill-volume summary) interleaves
    /// with the events.
    #[arg(
        long = "lineage-events",
        value_name = "PATH",
        help_heading = "Metrics",
        conflicts_with_all = ["lineage", "explain", "dry_run", "dry_run_n"]
    )]
    pub lineage_events: Option<PathBuf>,

    /// Validate config and CXL without processing data
    #[arg(long, help_heading = "Validation")]
    pub dry_run: bool,

    /// Process only first N records per input (requires --dry-run)
    #[arg(short = 'n', long, help_heading = "Validation")]
    pub dry_run_n: Option<u64>,

    /// Write dry-run output to file instead of stdout
    #[arg(long, help_heading = "Validation")]
    pub dry_run_output: Option<PathBuf>,

    /// CXL module search path
    #[arg(long, help_heading = "Paths")]
    pub rules_path: Option<PathBuf>,

    /// Base directory for relative path resolution
    #[arg(long, help_heading = "Paths")]
    pub base_dir: Option<PathBuf>,

    /// Permit absolute paths in YAML config
    #[arg(long, help_heading = "Paths")]
    pub allow_absolute_paths: bool,

    /// Active environment name (sets CLINKER_ENV).
    #[arg(long, help_heading = "Environment")]
    pub env: Option<String>,

    /// Suppress stderr progress output
    #[arg(long, help_heading = "Output")]
    pub quiet: bool,

    /// Allow output file overwrite
    #[arg(long, help_heading = "Output")]
    pub force: bool,

    /// Log level: error, warn, info, debug, trace
    #[arg(long, default_value = "info", help_heading = "Output")]
    pub log_level: String,

    /// Directory to spool per-execution JSON metrics files.
    /// Overrides CLINKER_METRICS_SPOOL_DIR env var and pipeline.metrics.spool_dir in YAML.
    #[arg(long, help_heading = "Metrics")]
    pub metrics_spool_dir: Option<PathBuf>,

    /// Channel id (tenant folder under the workspace `[channel]` root) whose
    /// overlay to apply before execution. Resolves the tenant's manifest and
    /// per-target overlay, derives matching groups from its labels, and applies
    /// the layered `config`/`vars` clobber, the structural `overrides:` op
    /// stream, and `sources:` per-source patches. See `clinker channels resolve`.
    #[arg(long, help_heading = "Configuration")]
    pub channel: Option<String>,

    /// Force-include a group overlay by name (repeatable). Applies the group's
    /// `overrides` op stream and `config`/`vars` clobber regardless of its
    /// selector, with or without a channel. See `clinker channels resolve`.
    #[arg(long = "group", value_name = "NAME", help_heading = "Configuration")]
    pub groups: Vec<String>,

    /// Suppress selector-derived group membership; only explicit `--group`
    /// overlays apply.
    #[arg(long = "no-auto-groups", help_heading = "Configuration")]
    pub no_auto_groups: bool,
}

impl RunArgs {
    /// Validate the `--memory-limit` flag and resolve the string to inject into
    /// `pipeline.memory.limit`.
    ///
    /// `Ok(None)` when the flag is absent or its value is empty or
    /// whitespace-only — an ops wrapper that forwards an unset variable expands
    /// to `--memory-limit ""`, and that must fall through to the YAML budget
    /// exactly as an omitted flag would, not abort. `Ok(Some(_))` for a valid
    /// budget the caller injects; the value is the trimmed flag string, which the
    /// executor re-parses through the lenient `memory.limit` grammar. A strictly
    /// accepted value scales identically on that lenient path (held by the
    /// strict/lenient agreement test), so the byte budget is preserved. `Err` —
    /// naming the flag and echoing the value — only for a non-empty but malformed
    /// value (e.g. the decimal `4GB` where the binary `4G` was meant), which must
    /// fail loudly rather than silently collapse to the default and shrink a
    /// larger configured budget.
    pub fn resolved_memory_limit(
        &self,
    ) -> Result<Option<String>, clinker_plan::config::ConfigError> {
        let Some(raw) = self.mem_limit.as_deref() else {
            return Ok(None);
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        parse_memory_limit_bytes_strict(trimmed)
            .map(|_| Some(trimmed.to_string()))
            .map_err(|reason| {
                clinker_plan::config::ConfigError::Validation(format!(
                    "--memory-limit {raw:?} is not a valid memory budget: {reason}"
                ))
            })
    }

    /// Resolve batch_id from CLI flag or generate UUID v7.
    pub fn resolved_batch_id(&self) -> String {
        self.batch_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::now_v7().to_string())
    }
}

/// Subcommands for `clinker metrics`.
#[derive(Subcommand, Debug)]
pub enum MetricsCommands {
    /// Sweep the spool directory and append records to an NDJSON archive
    #[command(
        long_about = "\
Sweep a spool directory for per-execution JSON metrics files and append \
them to a consolidated NDJSON archive. Use --delete-after-collect to clean \
up spool files after successful collection.",
        after_long_help = "\
EXAMPLES:
  # Preview what would be collected
  clinker metrics collect --spool-dir /var/spool/clinker --output-file metrics.ndjson --dry-run

  # Collect and clean up spool files
  clinker metrics collect --spool-dir /var/spool/clinker --output-file metrics.ndjson --delete-after-collect"
    )]
    Collect(CollectArgs),
}

/// Arguments for `clinker metrics collect`.
#[derive(Parser, Debug)]
pub struct CollectArgs {
    /// Spool directory to sweep (required)
    #[arg(long, required = true)]
    pub spool_dir: PathBuf,

    /// NDJSON output file to append collected records to (required)
    #[arg(long, required = true)]
    pub output_file: PathBuf,

    /// Delete spool files after successfully appending them
    #[arg(long)]
    pub delete_after_collect: bool,

    /// Print what would be collected without writing anything
    #[arg(long)]
    pub dry_run: bool,
}

/// Arguments for `clinker explain`.
#[derive(Parser, Debug)]
pub struct ExplainArgs {
    /// Path to the pipeline YAML configuration file.
    /// Not required when using --code alone.
    pub config: Option<PathBuf>,

    /// Exact versioned address or unique dotted shorthand to explain.
    #[arg(long)]
    pub field: Option<String>,

    /// Channel id (tenant folder under the workspace `[channel]` root) whose
    /// overlay to apply before provenance lookup, mirroring `clinker run --channel`.
    #[arg(long)]
    pub channel: Option<String>,

    /// Force-include a group overlay by name (repeatable) before provenance
    /// lookup, mirroring `clinker run --group`.
    #[arg(long = "group", value_name = "NAME")]
    pub groups: Vec<String>,

    /// Suppress selector-derived group membership; only explicit `--group`
    /// overlays apply.
    #[arg(long = "no-auto-groups")]
    pub no_auto_groups: bool,

    /// Error/warning code to look up (e.g. "E103")
    #[arg(long)]
    pub code: Option<String>,

    /// Base directory for relative path resolution
    #[arg(long, default_value = ".")]
    pub base_dir: PathBuf,
}

fn pipeline_error_exit_code(error: &PipelineError) -> u8 {
    match error {
        PipelineError::Config(_)
        | PipelineError::Schema(_)
        | PipelineError::PlanDiagnostics { .. }
        | PipelineError::OverlayDiagnostics(_)
        | PipelineError::Compilation { .. }
        | PipelineError::Internal { .. }
        | PipelineError::SortOrderViolation { .. }
        | PipelineError::MergeSortOrderViolation { .. }
        | PipelineError::SchemaMismatch { .. }
        | PipelineError::CompositionDepthExceeded { .. }
        | PipelineError::CompositionBodyMissing { .. }
        | PipelineError::CompositionUnknownPort { .. }
        | PipelineError::CompositionBodyError { .. }
        | PipelineError::MemoryBudgetExceeded { .. }
        | PipelineError::UnsatisfiableMemoryBudget { .. }
        | PipelineError::CombineMissingMatch { .. }
        | PipelineError::CombineOutputCapExceeded { .. }
        | PipelineError::EnvelopeMultiHeaderConflict { .. }
        | PipelineError::EnvelopeHeaderGrainUnmatched { .. }
        | PipelineError::EnvelopeHeaderMultipleForGrain { .. } => 1,
        // Disk-cap exceedance (E320) is a resource-exhaustion halt — the run
        // filled its configured spill budget. Group it with the other
        // infrastructure failures (I/O, spill, full-volume) at exit 4 rather
        // than the config exit 1: the pipeline is valid, the host ran out of
        // the disk headroom the cap allotted.
        PipelineError::Io(_) | PipelineError::Spill(_) | PipelineError::SpillCapExceeded { .. } => {
            4
        }
        // Runtime data-quality halts sit between config (1) and infrastructure
        // (4). This includes values outside an exact range axis and configured
        // DLQ or declared-type error ceilings.
        PipelineError::Eval(_)
        | PipelineError::Accumulator { .. }
        | PipelineError::CombineRangeKeyOutOfRange { .. }
        | PipelineError::DlqRateExceeded { .. }
        | PipelineError::TypeErrorThresholdExceeded { .. } => 3,
        PipelineError::Format(_) | PipelineError::ThreadPool(_) | PipelineError::Multiple(_) => 4,
        // Diagnostic-carrier — never propagated as a top-level error; folded
        // into DLQ at the emission site. Treat as exit 4 defensively in case a
        // future caller surfaces it.
        PipelineError::CorrelationGroupOverflow { .. } => 4,
        // A shutdown signal unwound the run before it finished draining. 130
        // is the conventional "terminated by SIGINT" status.
        PipelineError::Interrupted => 130,
    }
}

fn classify_pipeline_error(error: &PipelineError) -> clinker_core_types::FailureClassification {
    use clinker_core_types::{FailureClassification, RetryAdvice};

    let registered = |code| {
        FailureClassification::for_code(code)
            .unwrap_or_else(|| FailureClassification::unknown_internal("unregistered failure"))
    };
    match error {
        PipelineError::Format(format_error) => {
            if let Some(code) = format_error.classification_code() {
                return registered(code);
            }
            if matches!(format_error, clinker_format::FormatError::Io(_)) {
                registered("infrastructure.runtime.transient")
            } else {
                registered("source.data.invalid")
            }
        }
        PipelineError::Config(_)
        | PipelineError::Schema(_)
        | PipelineError::PlanDiagnostics { .. }
        | PipelineError::OverlayDiagnostics(_)
        | PipelineError::Compilation { .. } => registered("admission.configuration.invalid"),
        PipelineError::Internal { .. }
        | PipelineError::MergeSortOrderViolation { .. }
        | PipelineError::CompositionDepthExceeded { .. }
        | PipelineError::CompositionBodyMissing { .. }
        | PipelineError::CompositionUnknownPort { .. }
        | PipelineError::SchemaMismatch { .. } => registered("runtime.invariant.plan_mismatch"),
        PipelineError::CompositionBodyError { inner, .. } => classify_pipeline_error(inner),
        PipelineError::Io(_) | PipelineError::ThreadPool(_) => {
            registered("infrastructure.runtime.transient")
        }
        PipelineError::Spill(_) => registered("runtime.resource.spill_failed"),
        PipelineError::SpillCapExceeded { .. } => registered("runtime.resource.spill_cap_exceeded"),
        PipelineError::Multiple(errors) => errors
            .iter()
            .map(classify_pipeline_error)
            .min_by_key(|classification| {
                let retry_rank = match classification.retry_advice() {
                    RetryAdvice::DoNotRetry => 0_u8,
                    RetryAdvice::PolicyRequired => 1,
                    RetryAdvice::RetryWithBackoff => 2,
                };
                (retry_rank, classification.code())
            })
            .unwrap_or_else(|| registered("runtime.invariant.unknown")),
        PipelineError::Eval(_)
        | PipelineError::Accumulator { .. }
        | PipelineError::SortOrderViolation { .. }
        | PipelineError::CombineMissingMatch { .. }
        | PipelineError::CombineRangeKeyOutOfRange { .. }
        | PipelineError::CombineOutputCapExceeded { .. }
        | PipelineError::EnvelopeMultiHeaderConflict { .. }
        | PipelineError::EnvelopeHeaderGrainUnmatched { .. }
        | PipelineError::EnvelopeHeaderMultipleForGrain { .. }
        | PipelineError::DlqRateExceeded { .. }
        | PipelineError::TypeErrorThresholdExceeded { .. }
        | PipelineError::CorrelationGroupOverflow { .. } => registered("source.data.invalid"),
        PipelineError::MemoryBudgetExceeded { .. } => {
            registered("runtime.resource.memory_budget_exceeded")
        }
        PipelineError::UnsatisfiableMemoryBudget { .. } => {
            registered("admission.configuration.memory_budget_unsatisfiable")
        }
        PipelineError::Interrupted => registered("runtime.invariant.unknown"),
    }
}

fn main() -> ExitCode {
    let attempts_invocation = std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "attempts");
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = if attempts_invocation && error.use_stderr() {
                1
            } else {
                u8::try_from(error.exit_code()).unwrap_or(1)
            };
            let _ = error.print();
            return ExitCode::from(exit_code);
        }
    };

    match &cli.command {
        Commands::Run(args) => {
            let filter = args
                .log_level
                .parse::<tracing_subscriber::filter::LevelFilter>()
                .unwrap_or(tracing_subscriber::filter::LevelFilter::INFO);
            if args.machine.is_some() {
                tracing_subscriber::fmt()
                    .with_max_level(filter)
                    .with_writer(std::io::stderr)
                    .with_ansi(false)
                    .init();
            } else {
                tracing_subscriber::fmt().with_max_level(filter).init();
            }

            // Install the process-wide SIGINT/SIGTERM handler before the
            // run starts so an interrupt during a long pipeline trips the
            // run's shutdown token. Idempotent — the first call wins.
            if let Err(e) = clinker_exec::pipeline::shutdown::install_signal_handler() {
                eprintln!("clinker: failed to install signal handler: {e}");
            }

            let machine = match MachineEmitter::admit(args) {
                Ok(machine) => machine,
                Err(message) => {
                    eprintln!("clinker: {message}");
                    return ExitCode::from(1);
                }
            };
            if let Some(emitter) = machine.as_ref()
                && let Err(error) = emitter.emit_started()
            {
                eprintln!("clinker: cannot write machine protocol: {error}");
                return ExitCode::from(4);
            }
            if let Some(emitter) = machine.as_ref()
                && let Err(error) = emitter.emit_progress_transition("planning")
            {
                eprintln!("clinker: cannot write machine protocol: {error}");
                return ExitCode::from(4);
            }

            // The executor is fully synchronous — call it directly.
            match run(args, machine.as_ref()) {
                Ok(code) => {
                    if let Some(emitter) = machine.as_ref()
                        && let Err(error) = emitter.emit_completed(code)
                    {
                        eprintln!("clinker: cannot write machine terminal event: {error}");
                        return ExitCode::from(4);
                    }
                    ExitCode::from(code)
                }
                Err(e) => {
                    let exit_code = pipeline_error_exit_code(&e);
                    if let Some(emitter) = machine.as_ref() {
                        let terminal_result = if matches!(e, PipelineError::Interrupted) {
                            emitter.emit_completed(exit_code)
                        } else {
                            emitter.emit_failed(exit_code, &classify_pipeline_error(&e))
                        };
                        if let Err(error) = terminal_result {
                            eprintln!("clinker: cannot write machine terminal event: {error}");
                        }
                    }
                    render_pipeline_error(&e, &args.config);
                    ExitCode::from(exit_code)
                }
            }
        }
        Commands::Metrics { subcommand } => {
            tracing_subscriber::fmt()
                .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
                .init();
            match run_metrics(subcommand) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("clinker metrics error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Commands::Attempts { subcommand } => {
            tracing_subscriber::fmt()
                .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
                .init();
            match run_attempts(subcommand) {
                Ok(code) => ExitCode::from(code),
                Err(error) => {
                    eprintln!("clinker attempts error: {error}");
                    ExitCode::FAILURE
                }
            }
        }
        Commands::Explain(args) => {
            tracing_subscriber::fmt()
                .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
                .init();
            match run_explain(args) {
                Ok(()) => ExitCode::SUCCESS,
                // A plan diagnostic has already been rendered in full; saying
                // it again in one flat line would bury the report above it.
                Err(e) if e.is::<AlreadyReported>() => ExitCode::FAILURE,
                Err(e) => {
                    eprintln!("clinker explain error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Commands::Channels { subcommand } => {
            tracing_subscriber::fmt()
                .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
                .init();
            let result = match subcommand {
                ChannelsCommands::Resolve(args) => run_channels_resolve(args),
                ChannelsCommands::Lint(args) => run_channels_lint(args),
                ChannelsCommands::Group {
                    subcommand: GroupCommands::Members(args),
                } => run_channels_group_members(args),
                ChannelsCommands::Label {
                    subcommand: LabelCommands::Set(args),
                } => run_channels_label_set(args),
            };
            match result {
                Ok(code) => ExitCode::from(code),
                // A plan diagnostic has already been rendered in full; saying
                // it again in one flat line would bury the report above it.
                Err(e) if e.is::<AlreadyReported>() => ExitCode::FAILURE,
                Err(e) => {
                    eprintln!("clinker channels error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Commands::Refactor { subcommand } => {
            tracing_subscriber::fmt()
                .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
                .init();
            let result = match subcommand {
                RefactorCommands::RenameNode(args) => refactor::run_rename_node(args),
            };
            match result {
                Ok(code) => ExitCode::from(code),
                Err(e) => {
                    eprintln!("clinker refactor error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Commands::Config(args) => {
            tracing_subscriber::fmt()
                .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
                .init();
            match run_config(args) {
                Ok(code) => ExitCode::from(code),
                Err(e) => {
                    eprintln!("clinker config error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

/// Hand-rolled `Error + Diagnostic` wrapper so we can attach a
/// `NamedSource` without pulling in a new `thiserror` dependency
/// in the binary crate.
///
/// Carries the pieces miette renders separately: the stable diagnostic code
/// in the header, the help paragraph under the snippet, and a label pointing
/// into the attached source. A `PipelineError` with no structured diagnostic
/// behind it fills in the generic `clinker::pipeline_error` code and no
/// label, which is how every error rendered before plan diagnostics were
/// carried whole.
struct WrappedPipelineError {
    /// Prefixed onto the message. `None` when the snippet header already
    /// prints the path, so the filename is stated once per report.
    filename: Option<String>,
    code: String,
    severity: miette::Severity,
    message: String,
    help: Option<String>,
    labels: Vec<miette::LabeledSpan>,
    src: Option<std::sync::Arc<miette::NamedSource<String>>>,
}

impl WrappedPipelineError {
    fn prefixed_message(&self) -> String {
        match (&self.filename, self.severity) {
            // "pipeline error in ..." on a warning contradicts the glyph
            // miette prints beside it. Name the file without calling the
            // advisory a failure.
            (Some(f), miette::Severity::Error) => {
                format!("pipeline error in {f}: {}", self.message)
            }
            (Some(f), _) => format!("in {f}: {}", self.message),
            (None, _) => self.message.clone(),
        }
    }
}

impl std::fmt::Debug for WrappedPipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.filename {
            Some(name) => write!(f, "{name}: {}", self.message),
            None => write!(f, "{}", self.message),
        }
    }
}

impl std::fmt::Display for WrappedPipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.prefixed_message())
    }
}

impl std::error::Error for WrappedPipelineError {}

impl miette::Diagnostic for WrappedPipelineError {
    fn code<'a>(&'a self) -> Option<Box<dyn std::fmt::Display + 'a>> {
        Some(Box::new(&self.code))
    }
    fn severity(&self) -> Option<miette::Severity> {
        Some(self.severity)
    }
    fn help<'a>(&'a self) -> Option<Box<dyn std::fmt::Display + 'a>> {
        self.help
            .as_ref()
            .map(|h| Box::new(h) as Box<dyn std::fmt::Display + 'a>)
    }
    fn labels(&self) -> Option<Box<dyn Iterator<Item = miette::LabeledSpan> + '_>> {
        if self.labels.is_empty() {
            return None;
        }
        Some(Box::new(self.labels.clone().into_iter()))
    }
    fn source_code(&self) -> Option<&dyn miette::SourceCode> {
        self.src.as_deref().map(|s| s as &dyn miette::SourceCode)
    }
}

/// Byte range of 1-based `line` in `text`, excluding the line terminator.
///
/// Plan-time diagnostics anchor to a YAML line rather than a byte range:
/// serde-saphyr loses node-header byte offsets through the tagged-enum +
/// flatten shape the `nodes:` taxonomy uses, so the compile path stamps
/// `Span::line_only`. That is enough to point miette's snippet at the
/// offending node.
///
/// The carriage return of a CRLF-saved file is excluded along with the `\n`:
/// including it would run the underline one column past the line's last
/// visible character.
fn line_byte_range(text: &str, line: u32) -> Option<(usize, usize)> {
    let target = usize::try_from(line).ok()?.checked_sub(1)?;
    let bytes = text.as_bytes();
    let mut current = 0usize;
    let mut start = 0usize;

    while current < target {
        let mut i = start;
        while i < bytes.len() && bytes[i] != b'\n' && bytes[i] != b'\r' {
            i += 1;
        }
        if i == bytes.len() {
            return None;
        }
        start = if bytes[i] == b'\r' && bytes.get(i + 1) == Some(&b'\n') {
            i + 2
        } else {
            i + 1
        };
        current += 1;
    }

    let mut end = start;
    while end < bytes.len() && bytes[end] != b'\n' && bytes[end] != b'\r' {
        end += 1;
    }
    Some((start, end - start))
}

/// Whether every plan diagnostic's line anchor is provably a line of the
/// pipeline file, so a source snippet can be drawn from it.
///
/// A plan-time span is a bare line number with no file identity, so a snippet
/// is only correct while the text being quoted is the text that was parsed.
/// Two things break that. A composition body's gates number lines in the body
/// file, not the pipeline. And an overlay that rewrites the config — a
/// structural op, a source patch, a composition `config:` fold — leaves the
/// compiler working on something the file on disk no longer describes, so a
/// line resolved against it quotes stale content even where the numbering is
/// unchanged. Their absence is a proof, and their presence sends the whole run
/// down the unanchored path rather than risk quoting the wrong YAML.
///
/// Deliberately whole-run rather than per-diagnostic: attributing an
/// individual span would need file identity the span does not carry. Carrying
/// it is the better fix and is tracked separately.
fn plan_line_anchors_trusted(
    config: &clinker_plan::config::PipelineConfig,
    overlay_active: bool,
) -> bool {
    use clinker_plan::config::PipelineNode;
    !overlay_active
        && !config
            .nodes
            .iter()
            .any(|n| matches!(n.value, PipelineNode::Composition { .. }))
}

/// Marker for a failure whose diagnostic has already been printed by
/// [`render_pipeline_error`].
///
/// `run_explain` and `run_channels_resolve` return `Box<dyn Error>` and their
/// callers print whatever comes back. Returning this instead of a message keeps
/// them from printing a second, flatter version of a report the user has
/// already been shown.
#[derive(Debug)]
struct AlreadyReported;

impl std::fmt::Display for AlreadyReported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never surfaced: the caller checks for this type before printing.
        f.write_str("plan compilation failed")
    }
}

impl std::error::Error for AlreadyReported {}

/// Whether an overlay changed the config that was compiled.
///
/// The rule a snippet has to satisfy is that the text being quoted is the text
/// that was parsed. `--channel`/`--group` being passed does not decide that,
/// and neither does whether the overlay resolved to anything: a channel that
/// contributes only var overlays leaves the pipeline file byte-for-byte what
/// the compiler saw, so its snippets are correct and dropping them costs the
/// author a source line for nothing.
///
/// What does decide it is whether the overlay rewrote the document — a
/// structural op, a source patch, or a composition `config:` fold. Any of those
/// and the compiled config is no longer the file on disk, so a line number
/// resolved against that file quotes the wrong content. See
/// [`OverlayResolution::modifies_compiled_config`], which owns the list.
fn overlay_contributed(overlay: Option<&clinker_channel::OverlayResolution>) -> bool {
    overlay.is_some_and(clinker_channel::OverlayResolution::modifies_compiled_config)
}

/// Build the plan-diagnostic error, keeping line anchors only when
/// [`plan_line_anchors_trusted`] vouches for them.
fn plan_diagnostics(
    diags: Vec<clinker_core_types::Diagnostic>,
    anchors_trusted: bool,
) -> PipelineError {
    if anchors_trusted {
        PipelineError::plan_diagnostics(diags)
    } else {
        PipelineError::plan_diagnostics_unanchored(diags)
    }
}

/// Translate a diagnostic's severity into miette's, so a warning is not
/// painted as a failure.
///
/// `compile()` returns warnings alongside errors in its `Err` vector by
/// design, and the whole vector is rendered. Without this a `W002` advisory
/// prints as a red `×` indistinguishable from the error that actually stopped
/// the run.
fn miette_severity(severity: clinker_core_types::Severity) -> miette::Severity {
    match severity {
        clinker_core_types::Severity::Error => miette::Severity::Error,
        clinker_core_types::Severity::Warning => miette::Severity::Warning,
        clinker_core_types::Severity::Note => miette::Severity::Advice,
    }
}

/// Append `See: clinker explain --code <CODE>` to `help`.
///
/// Only when a page exists for `code` — the pointer is a promise that the
/// command answers, and `explain --code` on a code with no page reports it as
/// unknown.
///
/// Gates that already spell the pointer themselves keep the one they wrote, so
/// a report never says the same thing twice. `message` is checked alongside
/// `help` because the two families put it in different places: a structured
/// diagnostic attaches it to its help, while a `ConfigError::Validation`
/// carries it at the end of the message text.
fn with_explain_pointer(code: &str, message: &str, help: Option<String>) -> Option<String> {
    let pointer = format!("clinker explain --code {code}");
    if message.contains(&pointer)
        || help.as_deref().is_some_and(|h| h.contains(&pointer))
        || clinker_plan::plan::explain_provenance::explain_code(code).is_none()
    {
        return help;
    }
    Some(match help {
        Some(h) => format!("{h}\nSee: {pointer}"),
        None => format!("See: {pointer}"),
    })
}

/// Split a message that opens with its own registered `[CODE]` into that code
/// and the body after it.
///
/// A large family of plan-time gates reports through
/// `ConfigError::Validation(format!("[E346] ..."))` rather than as a structured
/// `Diagnostic`, so the code the user needs is carried in the message text.
/// `None` for a message that names no code, or names one the registry does not
/// list — an unregistered `[...]` is ordinary prose, not a code.
fn split_leading_code(message: &str) -> Option<(&str, &str)> {
    let (code, body) = message.strip_prefix('[')?.split_once(']')?;
    clinker_core_types::diagnostic::is_registered(code).then(|| (code, body.trim_start()))
}

/// Turn one of a diagnostic's spans into a miette label against `text`.
fn plan_label(
    span: clinker_core_types::span::Span,
    label: Option<&str>,
    fallback: &str,
    text: &str,
) -> Option<miette::LabeledSpan> {
    let (start, len) = line_byte_range(text, span.synthetic_line_number()?)?;
    let text = match label {
        Some(l) if !l.is_empty() => l.to_owned(),
        _ => fallback.to_owned(),
    };
    Some(miette::LabeledSpan::new(Some(text), start, len))
}

/// Render one plan-time diagnostic with its code, help, and source lines.
///
/// `source` is the pipeline file's text, and is supplied only when the plan is
/// a single document — see [`plan_line_anchors_trusted`]. Every secondary
/// label is rendered alongside the primary one: a two-location diagnostic such
/// as E164 is unactionable with only half of it shown, because the node the
/// author has to change is the one the secondary label points at.
///
/// The `See: clinker explain --code <CODE>` pointer is appended only when an
/// explain page exists, matching the wording the storage-validation and
/// staging-copy errors already use, so a user meets one phrasing across every
/// coded failure.
fn render_plan_diagnostic(
    diag: &clinker_core_types::Diagnostic,
    filename: Option<&str>,
    source: Option<&std::sync::Arc<miette::NamedSource<String>>>,
    source_text: Option<&str>,
) {
    let mut labels = Vec::new();
    if let Some(text) = source_text {
        if let Some(primary) = plan_label(
            diag.primary.span,
            diag.primary.label.as_deref(),
            "declared here",
            text,
        ) {
            labels.push(primary);
        }
        for secondary in &diag.secondary {
            if let Some(l) = plan_label(
                secondary.span,
                secondary.label.as_deref(),
                "related location",
                text,
            ) {
                labels.push(l);
            }
        }
    }

    // Some messages open with their own `[CODE]` prefix -- the composition-body
    // patch pass re-emits an op failure that way, and the CXL wrap carries the
    // resolver's code through. The code is already the report header, so a
    // prefix here would state it twice.
    let message = diag
        .message
        .strip_prefix(&format!("[{}]", diag.code))
        .map_or_else(|| diag.message.clone(), |rest| rest.trim_start().to_owned());

    let help = with_explain_pointer(&diag.code, &message, diag.help.clone());

    let has_labels = !labels.is_empty();
    let wrapped = WrappedPipelineError {
        // With a snippet the header already prints the path; without one the
        // message is the only place the failing pipeline is named. A caller
        // that cannot vouch for the file passes `None` rather than have the
        // report blame a document that is not at fault.
        filename: (!has_labels)
            .then_some(filename)
            .flatten()
            .map(str::to_owned),
        code: diag.code.clone(),
        severity: miette_severity(diag.severity),
        message,
        help,
        // A label needs its source attached; without one miette renders the
        // message alone, which is what a spanless diagnostic deserves.
        src: has_labels.then(|| source.cloned()).flatten(),
        labels,
    };
    eprintln!("{:?}", miette::Report::new(wrapped));
}

/// Renders a `PipelineError` via miette with the YAML source attached
/// as a `NamedSource`, falling back to plain-text output when the
/// config file is unreadable.
///
/// Every rendered diagnostic carries the source filename so CLI
/// output contains the `.yaml` path as part of the message or the
/// attached `NamedSource` header. The regression test
/// `test_diagnostic_renders_via_miette_in_cli` asserts that stderr
/// contains the config filename when a bad YAML is passed.
///
/// A `PlanDiagnostics` error renders one report per diagnostic so each keeps
/// its own code, help paragraph, and source line; everything else renders as
/// one report under the generic `clinker::pipeline_error` code.
fn render_pipeline_error(err: &PipelineError, config_path: &std::path::Path) {
    // Best-effort source attach. If the config file is unreadable
    // we still render the raw error via miette's graphical handler
    // so the user sees consistent formatting.
    //
    // Quoting is always from the RAW file, never the interpolated text.
    //
    // A span's line number indexes the interpolated config, because the loader
    // substitutes `${VAR}` before parsing. That tempts a renderer into quoting
    // the interpolated text so the numbers line up — but a `${SFTP_URL}`
    // holding a credential is resolved there, and miette prints the underlined
    // line plus one either side, so the secret would reach stderr and any log
    // capturing it. The raw file carries `${SFTP_URL}` literally, so nothing
    // sensitive can be printed from it on any path.
    //
    // The two texts share line numbering exactly when no substituted value
    // contained a YAML line break — the ordinary case — so the raw file quotes the
    // right line there. When a value did span lines the numberings diverge,
    // and rather than quote what is now the wrong line, the snippet is dropped
    // and the report renders without one.
    let source_text = std::fs::read_to_string(config_path).ok();
    // Interpolated only to ask whether any substituted value carried a YAML
    // line break; the expanded text is dropped here and never rendered. A
    // substitution that fails means the config never parsed, so there are no
    // plan diagnostics to anchor and no snippet to lose.
    //
    // The `&[]` mirrors what the loader passes. Should a caller ever supply
    // extra vars, they have to be threaded here too: a line break inside one
    // would shift the loader's line numbering without shifting the numbering
    // this asks about, and the snippet would quote the wrong line again.
    let anchor_text = source_text.as_deref().filter(|raw| {
        clinker_plan::config::interpolate_env_vars_with_metadata(raw, &[])
            .is_ok_and(|i| !i.shifted_lines)
    });
    let filename = config_path.to_string_lossy().into_owned();
    // Built once and shared: a compile that fails N gates would otherwise
    // clone the whole config text N times.
    let source = source_text
        .as_ref()
        .map(|s| std::sync::Arc::new(miette::NamedSource::new(filename.clone(), s.clone())));

    match err {
        PipelineError::PlanDiagnostics {
            diagnostics,
            anchors_trusted,
        } => {
            let (diagnostic_source, diagnostic_text) = if *anchors_trusted {
                (source.as_ref(), anchor_text)
            } else {
                (None, None)
            };
            for diag in diagnostics {
                render_plan_diagnostic(diag, Some(&filename), diagnostic_source, diagnostic_text);
            }
            return;
        }
        // The fault is in a channel/group file, which each message names for
        // itself. Attributing it to the pipeline file would send the author to
        // a document with nothing wrong in it, and no anchor here indexes the
        // pipeline text, so no snippet is drawn either.
        PipelineError::OverlayDiagnostics(diags) => {
            for diag in diags {
                render_plan_diagnostic(diag, None, None, None);
            }
            return;
        }
        _ => {}
    }

    // A large family of gates reports through `ConfigError::Validation` with
    // its code at the head of the message rather than as a structured
    // diagnostic. Rendering those under the placeholder
    // `clinker::pipeline_error` drops the code out of the header and the
    // explain pointer off the report, while the pages this PR adds describe
    // the fuller shape. The code is already in hand, so lift it and give the
    // message the same treatment a structured diagnostic gets.
    let validation = match err {
        PipelineError::Config(clinker_plan::config::ConfigError::Validation(msg)) => {
            split_leading_code(msg)
        }
        _ => None,
    };
    let (code, message, help) = match validation {
        Some((code, body)) => (
            code.to_owned(),
            body.to_owned(),
            with_explain_pointer(code, body, None),
        ),
        None => (
            String::from("clinker::pipeline_error"),
            err.to_string(),
            None,
        ),
    };

    let wrapped = WrappedPipelineError {
        filename: Some(filename),
        code,
        severity: miette::Severity::Error,
        message,
        help,
        labels: Vec::new(),
        src: source,
    };

    eprintln!("{:?}", miette::Report::new(wrapped));
}

/// Print every non-error diagnostic from a channel-overlay result and turn
/// any error-severity entry into a `PipelineError::PlanDiagnostics` so the
/// run aborts before executor init.
///
/// Errors are handed on whole rather than printed here: the top-level
/// renderer gives them the same code / help / source-line treatment as any
/// other plan-time diagnostic, and printing them here as well would show each
/// failure twice.
///
/// The anchors are dropped unconditionally, which is this path's correct
/// verdict rather than a blanket precaution: every diagnostic here is produced
/// while applying the channel/group overlay, so its line -- if it ever carries
/// one -- numbers the overlay file, never the pipeline. Threading the caller's
/// [`plan_line_anchors_trusted`] result would anchor them whenever the overlay
/// resolved to nothing, which is the one case that verdict returns `true` for.
/// Inert today because every channel diagnostic uses a synthetic span; stated
/// here so it stays correct when one does not.
fn abort_on_overlay_errors(
    overlay: &clinker_channel::ChannelOverlayResult,
) -> Result<(), PipelineError> {
    use clinker_core_types::Severity;
    let mut errors: Vec<clinker_core_types::Diagnostic> = Vec::new();
    for d in &overlay.diagnostics {
        match d.severity {
            Severity::Error => errors.push(d.clone()),
            Severity::Warning => eprintln!("warning: [{}] {}", d.code, d.message),
            Severity::Note => eprintln!("note: [{}] {}", d.code, d.message),
        }
    }
    if !errors.is_empty() {
        return Err(PipelineError::overlay_diagnostics(errors));
    }
    Ok(())
}

/// Compile the target once without config clobbers, validate every authored
/// channel/group config candidate against that typed provenance, and install
/// only the resulting validated fold into the executable compile context.
fn resolve_overlay_config_before_compile(
    config: &clinker_plan::config::PipelineConfig,
    compile_ctx: &mut clinker_plan::config::CompileContext,
    resolution: &clinker_channel::OverlayResolution,
) -> Result<(), PipelineError> {
    let mut validation_ctx = compile_ctx.clone();
    validation_ctx.config_overrides.clear();
    let validation_plan = config.compile(&validation_ctx).map_err(|diagnostics| {
        plan_diagnostics(
            diagnostics,
            plan_line_anchors_trusted(config, overlay_contributed(Some(resolution))),
        )
    })?;
    let resolved = resolution
        .resolve_config(&validation_plan)
        .map_err(PipelineError::overlay_diagnostics)?;
    compile_ctx.config_overrides = resolved.into_compile_overrides();
    Ok(())
}

/// Resolve the `(workspace_root, pipeline_dir)` pair for a `run` /
/// `run --explain` compile context from the pipeline file `config` and an
/// optional `--base-dir`.
///
/// Upholds the invariant `workspace_root.join(pipeline_dir) == config`'s
/// parent directory: that reconstructed directory is the anchor relative
/// source `path:` strings resolve against at compile time, and it must equal
/// the runtime source-discovery anchor (the pipeline file's directory) so a
/// file-size estimate computed at compile time names the same bytes the run
/// actually reads. The anchor is independent of the process CWD, keeping the
/// estimate reproducible across machines and launch directories.
///
/// `--base-dir` selects the workspace root used for the `.comp.yaml` scan and
/// composition `use:` resolution; absent, it defaults to the pipeline file's
/// own directory (`pipeline_dir` then empty). When a `--base-dir` is supplied
/// that is an ancestor of the pipeline file, `pipeline_dir` is the pipeline
/// file's directory expressed relative to it, so the join still reconstructs
/// the pipeline file's directory. Paths are canonicalized when they exist so
/// the result is symlink- and `..`-stable; a non-existent path falls back to
/// its lexical form rather than failing the run.
fn resolve_compile_anchor(
    config: &std::path::Path,
    base_dir: Option<&std::path::Path>,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let config_dir = config
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_path_buf();
    let config_dir = config_dir.canonicalize().unwrap_or(config_dir);
    let workspace_root = match base_dir {
        Some(base) => base.canonicalize().unwrap_or_else(|_| base.to_path_buf()),
        None => config_dir.clone(),
    };
    let pipeline_dir = config_dir
        .strip_prefix(&workspace_root)
        .unwrap_or_else(|_| std::path::Path::new(""))
        .to_path_buf();
    (workspace_root, pipeline_dir)
}

/// Map a workspace `[storage]` configuration failure onto the top-level
/// `PipelineError::Config` so it renders through the same miette path as a
/// pipeline-YAML validation error and exits with the config status code.
///
/// The `StorageConfigError` Display already names the offending
/// `storage.spill.dir` path and the underlying OS reason, so no span is
/// attached — the failing setting lives in `clinker.toml`, not the pipeline
/// YAML the diagnostic renderer carries as its `NamedSource`.
fn storage_config_error(e: clinker_plan::config::StorageConfigError) -> PipelineError {
    PipelineError::Config(clinker_plan::config::ConfigError::Validation(e.to_string()))
}

/// Map a comprehensive run-startup storage-validation failure onto the
/// top-level `PipelineError::Config`, the same path the config-time storage
/// errors take, so it renders through the shared miette diagnostic surface and
/// exits with the config status code.
///
/// The `StorageValidationError` Display already carries the stable diagnostic
/// code (E330–E334), the offending `clinker.toml` field, and the
/// `clinker explain --code <CODE>` pointer, so no span is attached — the
/// failing setting lives in `clinker.toml`, not the pipeline YAML the renderer
/// carries as its `NamedSource`.
fn storage_validation_error(e: clinker_exec::executor::StorageValidationError) -> PipelineError {
    PipelineError::Config(clinker_plan::config::ConfigError::Validation(e.to_string()))
}

/// Map a source-staging copy failure into the run's error type.
///
/// Staging is a run-setup concern (it happens before any record flows), so a
/// failure surfaces as a config-style validation error carrying the
/// staging-copy engine's full message — which already distinguishes a BLAKE3
/// verify mismatch, a disk-cap overflow, and a plain I/O failure.
fn staging_error(e: clinker_channel::StagingError) -> PipelineError {
    PipelineError::Config(clinker_plan::config::ConfigError::Validation(e.to_string()))
}

struct RunAttemptAbandonGuard(clinker_exec::output::attempt::RunAttemptPublication);

impl Drop for RunAttemptAbandonGuard {
    fn drop(&mut self) {
        let _ = self.0.abandon();
    }
}

fn insert_publication_root(
    roots: &mut std::collections::BTreeMap<
        std::path::PathBuf,
        clinker_plan::security::ValidatedPath,
    >,
    destination: &std::path::Path,
) -> Result<(), PipelineError> {
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let base = std::env::current_dir().map_err(PipelineError::Io)?;
    let root = clinker_plan::security::validate_path(parent, &base, parent.is_absolute()).map_err(
        |diagnostic| {
            PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
                "{}: {}",
                diagnostic.code, diagnostic.message
            )))
        },
    )?;
    roots.insert(root.as_path().to_path_buf(), root);
    Ok(())
}

fn run_unix_ms() -> Result<u64, PipelineError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| {
            PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                "system clock is earlier than the Unix epoch".to_owned(),
            ))
        })?;
    u64::try_from(duration.as_millis()).map_err(|_| {
        PipelineError::Config(clinker_plan::config::ConfigError::Validation(
            "system clock exceeds the supported publication range".to_owned(),
        ))
    })
}

#[derive(Clone)]
enum PublicationFailureKind {
    Readiness,
    ReadinessAndAbandonment,
    CleanupDebt(usize),
    Incomplete(usize),
    Publish(String),
}

fn publication_failure_diagnostic(execution_id: &str, failure: PublicationFailureKind) -> String {
    let reason = match failure {
        PublicationFailureKind::Readiness => "output readiness failed",
        PublicationFailureKind::ReadinessAndAbandonment => {
            "output readiness failed and the abandoned state could not be persisted"
        }
        PublicationFailureKind::CleanupDebt(count) => {
            return format!(
                "output publication completed with {count} cleanup debt item(s) for execution {execution_id}; run `clinker attempts inspect <pipeline> --execution-id {execution_id}` with the same identity options"
            );
        }
        PublicationFailureKind::Incomplete(count) => {
            return format!(
                "output publication was incomplete with {count} cleanup debt item(s) for execution {execution_id}; run `clinker attempts inspect <pipeline> --execution-id {execution_id}` with the same identity options"
            );
        }
        PublicationFailureKind::Publish(category) => {
            return format!(
                "output publication failed ({category}) for execution {execution_id}; run `clinker attempts inspect <pipeline> --execution-id {execution_id}` with the same identity options"
            );
        }
    };
    format!(
        "{reason} for execution {execution_id}; run `clinker attempts inspect <pipeline> --execution-id {execution_id}` with the same identity options"
    )
}

fn publication_error_category(error: &clinker_exec::output::attempt::AttemptError) -> String {
    use clinker_exec::output::attempt::AttemptError;

    match error {
        AttemptError::Containment(error) => {
            use clinker_exec::output::containment::ContainmentError;
            match error {
                ContainmentError::SecurityPolicy { code, .. } => {
                    format!("containment security policy {code}")
                }
                ContainmentError::PolicyRequired { profile, .. } => {
                    format!("unqualified destination profile {profile}")
                }
                ContainmentError::Io {
                    operation, source, ..
                } => format!(
                    "containment {operation}: kind={:?}, os_code={:?}",
                    source.kind(),
                    source.raw_os_error()
                ),
                ContainmentError::VisibleButUnsynced { source, .. } => format!(
                    "destination synchronization: kind={:?}, os_code={:?}",
                    source.kind(),
                    source.raw_os_error()
                ),
                ContainmentError::PublishedCleanup { .. } => {
                    "published cleanup operation".to_owned()
                }
            }
        }
        AttemptError::Pipeline(PipelineError::Io(source)) => format!(
            "I/O operation: kind={:?}, os_code={:?}",
            source.kind(),
            source.raw_os_error()
        ),
        AttemptError::Io {
            operation, source, ..
        } => format!(
            "{operation}: kind={:?}, os_code={:?}",
            source.kind(),
            source.raw_os_error()
        ),
        AttemptError::Pipeline(_) => "pipeline operation".to_owned(),
        AttemptError::Serialize(_) | AttemptError::Deserialize(_) => "manifest encoding".to_owned(),
        AttemptError::InvalidManifest(_)
        | AttemptError::InvalidTransition(_)
        | AttemptError::InvalidQuery(_)
        | AttemptError::InvalidContinuation(_) => "invalid durable state".to_owned(),
        AttemptError::Injected(_) | AttemptError::QualificationControl(_) => {
            "qualification control".to_owned()
        }
        AttemptError::RegistrationCollision { .. } => "destination collision".to_owned(),
        AttemptError::IntegrityMismatch { .. } => "integrity verification".to_owned(),
        AttemptError::AttemptByteLimitExceeded { .. }
        | AttemptError::AggregateAdmissionUnproven(_)
        | AttemptError::RetainedAttemptLimitExceeded { .. }
        | AttemptError::RetainedByteLimitExceeded { .. } => "publication admission".to_owned(),
    }
}

fn run(args: &RunArgs, machine: Option<&MachineEmitter>) -> Result<u8, PipelineError> {
    // Resolve CLINKER_ENV
    if let Some(env_name) = args
        .env
        .clone()
        .or_else(|| std::env::var("CLINKER_ENV").ok())
    {
        unsafe { std::env::set_var("CLINKER_ENV", env_name) };
    }

    // Resolve workspace_root and pipeline_dir ONCE at the entry point so
    // `compile()` never touches the process CWD. The invariant the compile
    // context must uphold is `workspace_root.join(pipeline_dir) ==` the
    // pipeline file's directory — that reconstructed directory is the
    // anchor every relative source `path:` resolves against, and it MUST
    // equal the runtime discovery anchor (`args.config.parent()` below at
    // the reader-registry build) so a file-size estimate computed at compile
    // time names the same bytes the run actually reads.
    let (workspace_root, pipeline_dir) =
        resolve_compile_anchor(&args.config, args.base_dir.as_deref());

    // Workspace `[storage]` config (clinker.toml at the workspace root).
    // The comprehensive run-startup validation (spill/staging filesystem-type
    // rejections, staging same-device, spill == staging, free-space preflight)
    // runs once on the run path below via `validate_storage_config`, after the
    // plan compiles and the source file set is discovered, and before any
    // source-ingest thread spawns. The lighter `spill.resolve()` here checks
    // only that a configured spill dir exists and is writable; it serves the
    // plan-only `--explain` display path (which never ingests, so the
    // filesystem-class rejections do not apply) and seeds the run path with the
    // same resolved root the validator re-derives.
    let clinker_toml = clinker_plan::config::ClinkerToml::load_from_workspace(&workspace_root)
        .map_err(storage_config_error)?;
    let group_layout = clinker_toml.group.clone();
    let catalog_config = clinker_toml.catalog.clone();
    let storage_config = clinker_toml.storage;
    let spill_root_dir = storage_config
        .spill
        .resolve()
        .map_err(storage_config_error)?;
    // Cumulative disk-spill quota (`storage.spill.disk_cap_bytes`). `None`
    // leaves the run's spill budget unlimited, the historical default; a
    // configured cap is folded into the arbitrator so a run that fills the
    // spill volume aborts with a dedicated cap diagnostic instead of an
    // out-of-memory message (the duckdb/duckdb#14142 trap).
    let spill_disk_cap_bytes = storage_config.spill.disk_cap();

    // Workspace `[storage.staging]` policy. Off by default; when enabled it
    // copies matched source files to a local volume before the run reads them.
    // Validated below against the discovered file set by
    // `validate_storage_config`, then driven per file by the staging-copy
    // engine at reader-open: a matched file is copied to a stable
    // content-addressed path under the staging root (`<source_id>.staged` plus a
    // `<source_id>.manifest.json` sidecar), single-pass BLAKE3 verify + atomic
    // publish, and the reader opens the local copy. The flat content-addressed
    // layout lets a later run reuse a still-fresh prior copy instead of
    // re-copying it per run.
    let staging_policy = storage_config.staging.clone();

    // Anchor for plan-derived lineage (--lineage): the pipeline file's directory,
    // i.e. the `workspace_root.join(pipeline_dir)` the compile context
    // reconstructs and against which relative source/output `path:` strings
    // resolve — so dataset names name the same bytes a run reads/writes. Both
    // halves are moved into `compile_ctx` next, so capture the join now.
    let lineage_base_dir = workspace_root.join(&pipeline_dir);

    // Resolve the channel/group overlay stack (op stream + config/vars clobber +
    // per-source patches) before loading the pipeline config, so the resolved
    // `sources:` patches can be applied to the parsed config pre-compile. A
    // `--channel <id>` selects a tenant by computed path (its manifest labels
    // drive selector-derived group membership); `--group <name>` force-includes
    // a group regardless of selector. An empty request (no channel, no groups)
    // resolves to nothing, keeping a plain run byte-identical.
    // Strip the full `.comp.yaml` / `.yaml` suffix (not just the last extension)
    // so a composition target `score.comp.yaml` resolves to the bare stem the
    // discovery layer expects — matching `channels resolve` / `channels lint`.
    let overlay_resolution = if args.channel.is_none() && args.groups.is_empty() {
        None
    } else {
        let catalog =
            clinker_plan::resources::WorkspaceCatalog::load(&workspace_root, &catalog_config)
                .map_err(|error| {
                    PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                        error.to_string(),
                    ))
                })?;
        let pipeline_id = catalog_pipeline_id(&workspace_root, &catalog_config, &args.config)
            .map_err(|error| {
                PipelineError::Config(clinker_plan::config::ConfigError::Validation(error))
            })?;
        Some(
            clinker_channel::resolve_target_channel(
                &workspace_root,
                &catalog,
                &group_layout,
                &pipeline_id,
                args.channel.as_deref(),
                &args.groups,
                !args.no_auto_groups,
            )
            .map_err(|e| {
                PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
                    "overlay resolution failed: {e}"
                )))
            })?,
        )
    };
    if let Some(res) = &overlay_resolution
        && !args.quiet
        && !res.is_empty()
    {
        eprintln!("clinker: applied overlay — {}", overlay_summary(res));
    }

    // Apply the resolved channel's per-source config patches (schema /
    // multi-value / options) to the parsed config before it is validated and
    // compiled, so every run path — normal run, `--explain`, and `--lineage` —
    // observes the patched shape. An absent channel (or one whose per-target
    // overlay declares no `sources:` block) is an empty map, making this
    // equivalent to a plain validated load.
    let empty_patches = indexmap::IndexMap::new();
    let source_patches = overlay_resolution
        .as_ref()
        .and_then(|res| res.source_patches())
        .unwrap_or(&empty_patches);
    let mut pipeline_config =
        clinker_plan::config::load_config_with_vars_and_patches(&args.config, &[], source_patches)
            .map_err(PipelineError::Config)?;
    apply_cli_force_policy(&mut pipeline_config, args.force);

    // A `--memory-limit` flag overrides the pipeline's `memory.limit`, matching
    // the documented CLI-wins precedence. The flag is validated at the boundary,
    // before it can reach the plan: only a valid budget is injected, so a
    // malformed value fails loudly (naming the flag) instead of parsing to the
    // default downstream and silently shrinking a larger configured budget. An
    // absent flag — or an empty/whitespace-only value, as an ops wrapper produces
    // when it forwards an unset variable — resolves to `None`, leaving the YAML
    // value untouched (the arbitrator's own 512 MiB default still applies when
    // the YAML is silent too), so it never clobbers a configured budget. The
    // executor recomputes the byte budget from the injected string through the
    // identical grammar, so the validated verdict, not the byte count, matters here.
    if let Some(limit) = args
        .resolved_memory_limit()
        .map_err(PipelineError::Config)?
    {
        pipeline_config.pipeline.memory.limit = Some(limit);
    }

    // Every run mode needs a primary source identity. Reject an empty
    // top-level pipeline through the normal diagnostic path instead of letting
    // the runtime setup below panic. This is config inspection only: it does
    // not discover or open the source.
    if pipeline_config.source_configs().next().is_none() {
        return Err(PipelineError::Config(
            clinker_plan::config::ConfigError::Validation(
                "pipeline must declare at least one source node".to_string(),
            ),
        ));
    }

    let mut compile_ctx =
        clinker_plan::config::CompileContext::with_pipeline_dir(workspace_root, pipeline_dir);
    compile_ctx.allow_absolute_paths = args.allow_absolute_paths;
    let clinker_plan::resources::CompositionDiscovery {
        fields: cxl_fields,
        identities: composition_body_identities,
    } = clinker_plan::resources::collect_cxl_fields_with_composition_identities(
        &pipeline_config.nodes,
        compile_ctx.workspace_root(),
        &compile_ctx.pipeline_dir,
    )
    .map_err(|error| {
        PipelineError::Config(clinker_plan::config::ConfigError::Validation(
            error.to_string(),
        ))
    })?;
    compile_ctx.composition_body_identities = composition_body_identities;
    let direct_imports =
        clinker_plan::resources::collect_direct_imports(&cxl_fields).map_err(|error| {
            PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                error.to_string(),
            ))
        })?;
    if !direct_imports.is_empty() {
        let catalog = clinker_plan::resources::WorkspaceCatalog::load(
            compile_ctx.workspace_root(),
            &catalog_config,
        )
        .map_err(|error| {
            PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                error.to_string(),
            ))
        })?;
        let rules_root = catalog
            .select_rules_root(
                args.rules_path.as_deref(),
                pipeline_config
                    .pipeline
                    .rules_path
                    .as_deref()
                    .map(std::path::Path::new),
            )
            .map_err(|error| {
                PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                    error.to_string(),
                ))
            })?;
        compile_ctx.cxl_modules = clinker_plan::resources::compile_module_closure(
            &catalog,
            &rules_root,
            &direct_imports,
            clinker_plan::resources::ModuleLimits::default(),
        )
        .map_err(|error| {
            PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                error.to_string(),
            ))
        })?;
    }
    if let Some(res) = &overlay_resolution {
        compile_ctx.overlay_ops = res.op_stream().to_vec();
    }

    // Run identity values flow through Output path templates and the
    // provenance sidecar. Generated before --explain so resolved-path
    // summaries match the values the actual run would use. The id pair
    // re-rolls per invocation; consumers correlate runs via batch_id.
    let execution_id = machine.map_or_else(
        || uuid::Uuid::now_v7().to_string(),
        |emitter| emitter.execution_id(),
    );
    let batch_id = machine.map_or_else(|| args.resolved_batch_id(), |emitter| emitter.batch_id());
    let pipeline_hash = pipeline_config.source_hash;
    let timestamp_str = chrono::Utc::now().format("%Y-%m-%dT%H-%M-%SZ").to_string();
    let mut source_name_by_node: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for src in pipeline_config.source_configs() {
        if !src.transport.is_file() {
            // A network source has no file path. Resolve `{source_file}`
            // to the same stable synthetic id the executor stamps on each
            // record (`<source:NAME>`) so fan-out templates render a
            // deterministic, source-identifying token instead of an empty
            // stem.
            source_name_by_node.insert(src.name.clone(), format!("<source:{}>", src.name));
        } else if let Some(stem) = std::path::Path::new(src.path_str())
            .file_stem()
            .and_then(|s| s.to_str())
        {
            source_name_by_node.insert(src.name.clone(), stem.to_string());
        }
    }
    let source_name_default: Option<String> =
        pipeline_config.source_configs().next().and_then(|s| {
            if !s.transport.is_file() {
                Some(format!("<source:{}>", s.name))
            } else {
                std::path::Path::new(s.path_str())
                    .file_stem()
                    .and_then(|st| st.to_str().map(|s| s.to_string()))
            }
        });
    let template_ctx = clinker_plan::config::path_template::TemplateContext {
        source_name_default: source_name_default.as_deref(),
        source_name_by_node: source_name_by_node.clone(),
        channel: overlay_resolution.as_ref().and_then(|r| r.channel_id()),
        pipeline_hash,
        timestamp: Some(&timestamp_str),
        execution_id: Some(&execution_id),
        batch_id: Some(&batch_id),
        n: None,
        unique_suffix_width: 0,
    };
    // --lineage names output datasets from the pipeline's declared output paths,
    // so snapshot the config before per-run tokens ({execution_id}/{timestamp}/…)
    // are baked into them just below; the export compiles from this snapshot to
    // keep dataset names reproducible across runs of the same pipeline.
    let lineage_config = args.lineage.as_ref().map(|_| pipeline_config.clone());
    clinker_plan::config::path_template::resolve_output_path_templates_in_place(
        &mut pipeline_config,
        &template_ctx,
    )
    .map_err(PipelineError::Config)?;

    // The resolved-outputs preamble is human-readable text decoration. The
    // text explain and the compile-validation dry run want it; the JSON and
    // DOT explain formats are machine-consumed, so emitting a non-JSON / non-
    // DOT preamble to stdout would make their output unparseable (the JSON
    // form exists precisely so downstream tooling can read the plan and the
    // storage summary without parsing prose).
    let preamble_wanted = match args.explain {
        Some(ExplainFormat::Text) => true,
        Some(ExplainFormat::Json) | Some(ExplainFormat::Dot) => false,
        None => args.dry_run && args.dry_run_n.is_none(),
    };
    if preamble_wanted {
        print_resolved_outputs(&pipeline_config);
    }

    if let Some(format) = args.explain {
        let mut effective_compile_ctx = compile_ctx.clone();
        if let Some(res) = &overlay_resolution {
            resolve_overlay_config_before_compile(
                &pipeline_config,
                &mut effective_compile_ctx,
                res,
            )?;
        }
        let mut compiled_plan = pipeline_config
            .compile(&effective_compile_ctx)
            .map_err(|d| {
                plan_diagnostics(
                    d,
                    plan_line_anchors_trusted(
                        &pipeline_config,
                        overlay_contributed(overlay_resolution.as_ref()),
                    ),
                )
            })?;
        let effective_runtime_variables = if let Some(res) = &overlay_resolution {
            let overlay = res.apply_config_and_vars(&mut compiled_plan, &pipeline_config);
            abort_on_overlay_errors(&overlay)?;
            clinker_plan::plan::EffectiveRuntimeVariables {
                static_vars: overlay.static_vars,
                pipeline_vars: overlay.pipeline_vars,
                source_vars: overlay.source_vars,
                record_vars: overlay.record_vars,
            }
        } else {
            clinker_plan::plan::EffectiveRuntimeVariables::default()
        };

        if let Some(emitter) = machine {
            let fingerprint = compiled_plan
                .semantic_fingerprint_with_runtime_variables(&effective_runtime_variables)
                .map_err(|error| PipelineError::Internal {
                    op: "machine semantic fingerprint",
                    node: "pipeline".to_owned(),
                    detail: error.to_string(),
                })?;
            emitter
                .emit_plan_resolved(fingerprint)
                .map_err(PipelineError::Io)?;
        }
        let dag = compiled_plan.dag();
        let statistics = compiled_plan.statistics();
        match format {
            ExplainFormat::Text => {
                print!(
                    "{}",
                    dag.explain_text_with_statistics(&pipeline_config, statistics)
                );
                // Resolved spill root: the directory under which the per-run
                // `clinker-spill-*` directory is created. Shows the configured
                // `storage.spill.dir` when set, the OS temp dir otherwise, so
                // an operator can confirm where blocking operators will spill
                // before committing to the run.
                let spill_root_display = spill_root_dir.clone().unwrap_or_else(std::env::temp_dir);
                let spill_root_source = if spill_root_dir.is_some() {
                    "storage.spill.dir"
                } else {
                    "OS temp dir (default)"
                };
                println!(
                    "Spill root: {} [{}]",
                    spill_root_display.display(),
                    spill_root_source
                );
                // Resolved disk-spill cap: the cumulative on-disk spill budget
                // (`storage.spill.disk_cap_bytes`), or unlimited when unset. An
                // operator can confirm the cap before a run that might fill the
                // spill volume — a cap hit aborts with E320, distinct from an
                // out-of-memory (E310) or a full volume (E321).
                match spill_disk_cap_bytes {
                    Some(cap) => {
                        println!("Spill disk cap: {cap} bytes [storage.spill.disk_cap_bytes]")
                    }
                    None => println!("Spill disk cap: unlimited (default)"),
                }
                // Resolved spill-compression decision per blocking operator.
                // Under `auto` the choice varies by operator width, so an
                // operator can confirm which spills will be LZ4-framed and
                // which write raw postcard before committing to the run.
                let batch_size = pipeline_config
                    .pipeline
                    .batch_size
                    .unwrap_or(clinker_exec::executor::DEFAULT_BATCH_SIZE);
                print!(
                    "{}",
                    dag.spill_compression_explain(storage_config.spill.compress, batch_size)
                );
                // Cap-headroom: the spill cap minus the run's estimated spill
                // volume. Surfaced only when a cap is configured; the figure is
                // per invocation and explicitly disclaims sibling invocations
                // sharing the volume under the partition-and-run model.
                print!(
                    "{}",
                    cap_headroom_explain(spill_disk_cap_bytes, dag.estimated_spill_bytes())
                );
                // Staging plan per source: whether each source (or each
                // discovered file under a multi-file matcher) would be staged,
                // the resolved staged path, and the reuse-if-fresh decision.
                // The discovery anchor matches the run path's
                // (`args.config.parent()`), so the staged paths shown are the
                // exact paths the real run would write.
                let discovery_anchor = args
                    .config
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| std::path::PathBuf::from("."));
                print!(
                    "{}",
                    staging_plan_explain(&pipeline_config, &staging_policy, &discovery_anchor)
                );
            }
            ExplainFormat::Json => {
                // Storage observability at parity with the text path: the
                // same per-stage spill estimate, spill root / disk cap,
                // compression decision, cap headroom, and staging plan,
                // structured so downstream tooling reads it without
                // re-parsing prose.
                let storage_summary = build_storage_summary_json(
                    dag,
                    &pipeline_config,
                    &storage_config,
                    spill_root_dir.as_deref(),
                    &args.config,
                );
                let view = clinker_plan::plan::execution::ExplainJson::new(dag, statistics)
                    .with_storage_summary(storage_summary);
                let json = serde_json::to_string_pretty(&view).map_err(|e| {
                    PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
                        "JSON serialization failed: {e}"
                    )))
                })?;
                println!("{json}");
            }
            ExplainFormat::Dot => {
                print!("{}", dag.explain_dot());
            }
        }
        return Ok(0);
    }

    // Plan-derived OpenLineage column lineage. Like --explain, compile the plan
    // and emit without reading any data, then exit. The export is static: its
    // runId is this invocation's execution_id and does NOT identify a
    // data-processing run (for live run-lifecycle events with real timing and row
    // counts, run the pipeline with --lineage-events instead). Compiles from the
    // pre-template snapshot so output dataset names are the declared paths, not
    // this run's resolved ones.
    if let Some(path) = &args.lineage {
        let cfg = lineage_config
            .as_ref()
            .expect("lineage_config is captured whenever --lineage is set");
        let mut effective_compile_ctx = compile_ctx.clone();
        if let Some(res) = &overlay_resolution {
            resolve_overlay_config_before_compile(cfg, &mut effective_compile_ctx, res)?;
        }
        let mut compiled_plan = cfg.compile(&effective_compile_ctx).map_err(|d| {
            plan_diagnostics(
                d,
                plan_line_anchors_trusted(cfg, overlay_contributed(overlay_resolution.as_ref())),
            )
        })?;
        let effective_runtime_variables = if let Some(res) = &overlay_resolution {
            let overlay = res.apply_config_and_vars(&mut compiled_plan, cfg);
            abort_on_overlay_errors(&overlay)?;
            clinker_plan::plan::EffectiveRuntimeVariables {
                static_vars: overlay.static_vars,
                pipeline_vars: overlay.pipeline_vars,
                source_vars: overlay.source_vars,
                record_vars: overlay.record_vars,
            }
        } else {
            clinker_plan::plan::EffectiveRuntimeVariables::default()
        };

        if let Some(emitter) = machine {
            let fingerprint = compiled_plan
                .semantic_fingerprint_with_runtime_variables(&effective_runtime_variables)
                .map_err(|error| PipelineError::Internal {
                    op: "machine semantic fingerprint",
                    node: "pipeline".to_owned(),
                    detail: error.to_string(),
                })?;
            emitter
                .emit_plan_resolved(fingerprint)
                .map_err(PipelineError::Io)?;
        }

        let lineage = clinker_lineage::column_lineage(&compiled_plan, &lineage_base_dir);
        let source_hash = clinker_exec::output::sidecar::hash_to_hex(&pipeline_hash);
        let job = clinker_lineage::Job::for_pipeline(cfg.pipeline.name.clone(), source_hash);
        let event_time = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let events = clinker_lineage::run_events(&lineage, &execution_id, job, &event_time);

        let writer: Box<dyn std::io::Write> = if path.as_os_str() == std::ffi::OsStr::new("-") {
            Box::new(std::io::stdout().lock())
        } else {
            Box::new(std::fs::File::create(path).map_err(|e| {
                PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
                    "cannot open --lineage output {}: {e}",
                    path.display()
                )))
            })?)
        };
        clinker_lineage::write_ndjson(&events, writer).map_err(PipelineError::Io)?;
        return Ok(0);
    }

    // Validate -n only valid with --dry-run
    if args.dry_run_n.is_some() && !args.dry_run {
        return Err(PipelineError::Config(
            clinker_plan::config::ConfigError::Validation(
                "-n/--dry-run-n requires --dry-run flag".to_string(),
            ),
        ));
    }

    // Resolve spool directory (CLI > env > YAML)
    let yaml_spool = pipeline_config
        .pipeline
        .metrics
        .as_ref()
        .and_then(|m| m.spool_dir.as_deref());
    let spool_dir = metrics::resolve_spool_dir(args.metrics_spool_dir.as_deref(), yaml_spool);

    // Channel/group-resolved var overrides land here when an overlay applies.
    // Populated below from the overlay resolution's `apply_config_and_vars`
    // after compile; the executor layers them atop Transform-declared defaults
    // at init.
    let mut channel_static_vars: indexmap::IndexMap<String, clinker_record::Value> =
        Default::default();
    let mut channel_pipeline_vars: indexmap::IndexMap<String, clinker_record::Value> =
        Default::default();
    let mut channel_source_vars: indexmap::IndexMap<
        String,
        indexmap::IndexMap<String, clinker_record::Value>,
    > = Default::default();
    let mut channel_record_vars: indexmap::IndexMap<String, clinker_record::Value> =
        Default::default();

    // Build the source reader registry. Each source's matcher
    // (`path` / `glob` / `regex` / `paths`) resolves through the
    // discovery layer; every matched file becomes one `FileSlot` and
    // the executor's `MultiFileFormatReader` concatenates them into
    // a single record stream stamped with `$source.file` per record.
    let mut readers: clinker_exec::executor::SourceReaders = std::collections::HashMap::new();
    let workspace_root = args
        .config
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    // Side-table: per-source discovered file paths, used by the
    // fan-out output setup below to pre-render `{source_file}` per
    // matched file. Mirrors the FileSlot Arcs the executor stamps on
    // each record so fan-out writers key correctly.
    let mut source_files_by_name: std::collections::HashMap<String, Vec<std::path::PathBuf>> =
        std::collections::HashMap::new();
    // Compile the plan before opening any reader so the run-startup storage
    // validation can read the plan's estimated spill volume for its free-space
    // preflight, and so output-side fan-out detection (§5) can read
    // `fan_out_per_source_file` flags before the writer setup decides whether
    // to open one writer or N.
    // The structural overlay op stream (if any) is applied inside `compile`
    // via `compile_ctx.overlay_ops`, so a bad splice anchor or ill-typed op is
    // a compile diagnostic, not a panic — propagate it rather than unwrapping.
    if let Some(res) = &overlay_resolution {
        resolve_overlay_config_before_compile(&pipeline_config, &mut compile_ctx, res)?;
    }
    let mut compiled_plan = pipeline_config.compile(&compile_ctx).map_err(|d| {
        plan_diagnostics(
            d,
            plan_line_anchors_trusted(
                &pipeline_config,
                overlay_contributed(overlay_resolution.as_ref()),
            ),
        )
    })?;
    // Channel/group overlay: config/vars clobber over the compiled plan's
    // provenance, resolving the four scoped var registries into the runtime
    // values the executor layers atop Transform-declared defaults at init.
    if let Some(res) = &overlay_resolution {
        let overlay = res.apply_config_and_vars(&mut compiled_plan, &pipeline_config);
        abort_on_overlay_errors(&overlay)?;
        channel_static_vars.extend(overlay.static_vars);
        channel_pipeline_vars.extend(overlay.pipeline_vars);
        for (src, inner) in overlay.source_vars {
            channel_source_vars.entry(src).or_default().extend(inner);
        }
        channel_record_vars.extend(overlay.record_vars);
    }

    let effective_runtime_variables = clinker_plan::plan::EffectiveRuntimeVariables {
        static_vars: channel_static_vars,
        pipeline_vars: channel_pipeline_vars,
        source_vars: channel_source_vars,
        record_vars: channel_record_vars,
    };

    if let Some(emitter) = machine {
        let fingerprint = compiled_plan
            .semantic_fingerprint_with_runtime_variables(&effective_runtime_variables)
            .map_err(|error| PipelineError::Internal {
                op: "machine semantic fingerprint",
                node: "pipeline".to_owned(),
                detail: error.to_string(),
            })?;
        emitter
            .emit_plan_resolved(fingerprint)
            .map_err(PipelineError::Io)?;
        emitter
            .emit_progress_transition("executing")
            .map_err(PipelineError::Io)?;
    }

    if args.dry_run && args.dry_run_n.is_none() {
        // Compile-validation mode (no -n): the plan and any channel/group
        // overlay are fully checked, but runtime source discovery, reader and
        // writer setup, and record processing do not begin. Compilation may
        // inspect source metadata for planning estimates.
        tracing::info!(
            "Dry run: plan valid, {} inputs, {} outputs, {} transforms",
            pipeline_config.source_configs().count(),
            pipeline_config.output_configs().count(),
            pipeline_config.transform_node_count(),
        );
        return Ok(0);
    }

    // Discovery pre-pass: resolve every File source's matcher to its file set
    // and build a Rest reader for every network source, before any storage
    // validation or staging copy. Collecting the full discovered file set up
    // front lets the run-startup validation below run once against all sources
    // (the staging same-device rule needs the complete matched set), rather
    // than per source.
    let mut discovered_files: Vec<(String, Vec<std::path::PathBuf>)> = Vec::new();
    let mut all_source_paths: Vec<std::path::PathBuf> = Vec::new();
    for body in pipeline_config.source_bodies() {
        let source = &body.source;
        match &source.transport {
            clinker_plan::config::SourceTransport::File => {
                let outcome = clinker_plan::config::discovery::discover(source, &workspace_root)
                    .map_err(|e| {
                        use clinker_plan::config::discovery::DiscoveryError;
                        let code = match &e {
                            DiscoveryError::MultipleMatchers { .. } => "E210",
                            DiscoveryError::NoMatcher => "E211",
                            DiscoveryError::InvalidGlob { .. } => "E212",
                            DiscoveryError::InvalidRegex { .. } => "E213",
                            DiscoveryError::NoMatch { .. } => "E216",
                            DiscoveryError::TakeBothSpecified => "E218",
                            DiscoveryError::Io(_) => "E216",
                        };
                        clinker_plan::error::PipelineError::Config(
                            clinker_plan::config::ConfigError::Validation(format!(
                                "[{code}] source '{}' discovery failed: {e}",
                                source.name
                            )),
                        )
                    })?;
                let paths: Vec<std::path::PathBuf> =
                    outcome.files().iter().map(|f| f.path.clone()).collect();
                all_source_paths.extend(paths.iter().cloned());
                discovered_files.push((source.name.clone(), paths));
            }
            clinker_plan::config::SourceTransport::Rest(rest_cfg) => {
                // The rest transport bypasses fs discovery entirely. The
                // reader is a row yielder driven on the ingest thread; the
                // `{source_file}` fan-out side-table gets no file paths, so
                // the `<source:NAME>` synthetic id is the stable identity.
                let rest_columns = body.schema.bound_columns().unwrap_or_default();
                let reader = clinker_net::build_rest_source(
                    rest_cfg.clone(),
                    source,
                    &rest_columns,
                    body.on_unmapped.clone(),
                )
                .map_err(clinker_plan::error::PipelineError::Format)?;
                source_files_by_name.insert(source.name.clone(), Vec::new());
                readers.insert(
                    source.name.clone(),
                    clinker_exec::executor::SourceInput::Records(reader),
                );
            }
        }
    }

    // Run-startup storage-config validation — the single, comprehensive pass.
    // Runs after the plan compiles and the source file set is discovered, and
    // before any source-ingest thread spawns or any staged copy is written, so
    // a spill dir on tmpfs/network (E330/E331), a staging dir on a network FS
    // (E332), a staging dir sharing a device with a staged source (E333), or a
    // spill dir equal to the staging dir (E334) fails the run at startup rather
    // than at the first spill or copy. The free-space preflight reads the
    // plan's estimated spill volume and warns (W330) — without aborting — when
    // the spill volume looks too small, a backstop separate from the runtime
    // disk cap (E320) and full-volume (E321) surfaces.
    let estimated_spill_bytes = compiled_plan.dag().estimated_spill_bytes();
    let resolved_storage = clinker_exec::executor::validate_storage_config(
        &storage_config,
        &all_source_paths,
        estimated_spill_bytes,
    )
    .map_err(storage_validation_error)?;
    let spill_root_dir = resolved_storage.spill_root_dir;
    if let Some(warning) = &resolved_storage.free_space_warning {
        tracing::warn!("{warning}");
        eprintln!("{warning}");
    }
    // Cap-headroom warning: the run's estimated spill volume is within 80% of
    // the configured `storage.spill.disk_cap_bytes`, so it is likely to abort
    // with E320 mid-stream. Fired here on the REAL run path — before any source
    // ingest — so the operator sees the signal at startup; advisory, not fatal.
    if let Some(warning) = &resolved_storage.cap_headroom_warning {
        tracing::warn!("{warning}");
        eprintln!("{warning}");
    }

    // Idempotent staging crash-purge, run once before this run stages. A
    // crashed prior run (SIGKILL, OOM-killer, power loss) skips the cleanup a
    // clean exit performs, leaking its staged artifacts under the staging root.
    // Best-effort: it reaps a `.partial` whose owning run is dead and any
    // `.staged` with no committed manifest. It IS concurrency-safe, so runs may
    // safely share a staging root: a per-source advisory lock (fs4) serializes
    // concurrent invocations of the same source — exactly one copies and the
    // rest reuse — and this purge is liveness-aware, reaping a `.partial` only
    // when its owner's lock is acquirable (the owner is gone) and the file has
    // aged past a creation grace window, never a live sibling's in-flight copy.
    // The staging root is always an explicitly configured local volume, so
    // unlike the spill purge (which skips the unconfigured OS-temp default) this
    // always runs when staging is enabled; it lives here because staging is a
    // CLI-only concern.
    clinker_channel::SourceStager::crash_purge(&staging_policy);

    // Stage + open pass: with validation passed, copy each matched source to
    // its stable content-addressed path under the staging root
    // (`<source_id>.staged` + `<source_id>.manifest.json`, single-pass BLAKE3
    // verify + atomic publish) and open the reader on the local copy, or open
    // the source in place when staging is disabled or no pattern matched. One
    // staging engine for the whole run reuses a still-fresh prior copy when the
    // manifest matches and accumulates the disk-cap byte total across every
    // source it actually copies.
    let mut source_stager = clinker_channel::SourceStager::new(staging_policy.clone());
    for (source_name, paths) in discovered_files {
        let mut slots: Vec<clinker_exec::source::multi_file::FileSlot> = Vec::new();
        for path in &paths {
            // A matched file is copied to its content-addressed local path and
            // `read_path()` points at the local copy; an unmatched file or a
            // disabled policy reads in place. Either way the reader opens
            // `read_path()` and stays agnostic to staging.
            let staged = source_stager.resolve(path.clone()).map_err(staging_error)?;
            let read_path = staged.read_path().to_path_buf();
            // `resolve` returned holding this source's shared advisory read lock
            // (retained inside `source_stager` for the run), so between that
            // return and this open a concurrent run's cleanup/overwrite — which
            // need the exclusive lock — cannot remove or replace the staged file.
            // `open_source_file` adds the Windows FILE_SHARE_DELETE share mode so
            // a concurrent atomic-rename publish or delete still interoperates
            // with this open handle on Windows.
            // Validate readability up front (surfacing a permission/missing
            // error here, before the executor thread starts) while leaving the
            // reader to re-open the stable staged `read_path` per pass. The
            // staged copy is held under this source's shared advisory read lock
            // for the run, so re-opens read byte-identical content.
            clinker_channel::open_source_file(&read_path)?;
            slots.push(clinker_exec::source::multi_file::FileSlot::from_path(
                path.clone(),
                read_path,
            ));
        }
        source_files_by_name.insert(source_name.clone(), paths);
        // EmptyWarn / EmptySkip outcomes leave `slots` empty; the executor
        // short-circuits via the empty-list guard upstream.
        if slots.is_empty() {
            // Stash a single empty reader so the executor's "missing reader"
            // check passes. Records flow through as zero-row sources.
            slots.push(clinker_exec::source::multi_file::FileSlot::new(
                "<empty>",
                Box::new(std::io::empty()),
            ));
        }
        readers.insert(
            source_name,
            clinker_exec::executor::SourceInput::Files(slots),
        );
    }

    // Every output writes to a hidden destination-local leaf admitted through
    // a retained containment boundary. The shared ledger also serves lazy
    // split writers, and the CLI publishes the complete ledger only after the
    // executor reports success. Failed runs leave existing finals untouched
    // and preserve hidden partials for inspection.
    let mut writers: std::collections::HashMap<String, Box<dyn std::io::Write + Send>> =
        std::collections::HashMap::new();
    let mut fan_out_destinations: std::collections::HashMap<
        String,
        Vec<(std::sync::Arc<str>, std::path::PathBuf, std::path::PathBuf)>,
    > = std::collections::HashMap::new();
    let mut historical_source_identities = std::collections::BTreeSet::new();
    for output in pipeline_config.output_configs() {
        if !output_is_fan_out(compiled_plan.dag(), &output.name) {
            continue;
        }
        let upstream_source = upstream_source_for_output(compiled_plan.dag(), &output.name);
        let files = upstream_source
            .as_ref()
            .and_then(|source| source_files_by_name.get(source.as_str()))
            .cloned()
            .unwrap_or_default();
        let mut identities: std::collections::HashMap<std::path::PathBuf, std::path::PathBuf> =
            std::collections::HashMap::new();
        let mut rendered = Vec::with_capacity(files.len());
        for source_path in files {
            if let Some(source_name) = &upstream_source {
                historical_source_identities.insert(
                    clinker_exec::output::attempt::HistoricalSourceIdentity::new(
                        source_name.clone(),
                        source_path.to_string_lossy().into_owned(),
                    )
                    .map_err(|error| {
                        PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                            error.to_string(),
                        ))
                    })?,
                );
            }
            let source_key: std::sync::Arc<str> =
                std::sync::Arc::from(source_path.to_string_lossy().into_owned());
            let source_file = source_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("source");
            let destination = std::path::PathBuf::from(
                output.render_runtime_path(source_file, &source_path.to_string_lossy())?,
            );
            if !output.authored_path_was_absolute() && destination.is_absolute() {
                return Err(PipelineError::Config(
                    clinker_plan::config::ConfigError::Validation(format!(
                        "fan-out output {:?} rendered an absolute path from a relative template",
                        output.name
                    )),
                ));
            }
            let identity = std::path::absolute(&destination)?;
            if let Some(first_source) = identities.insert(identity.clone(), source_path.clone()) {
                return Err(PipelineError::Config(
                    clinker_plan::config::ConfigError::Validation(format!(
                        "fan-out output {:?} renders both {} and {} to {}; include {{source_path}} or another distinguishing path component",
                        output.name,
                        first_source.display(),
                        source_path.display(),
                        identity.display(),
                    )),
                ));
            }
            rendered.push((source_key, destination, source_path));
        }
        fan_out_destinations.insert(output.name.clone(), rendered);
    }

    // Resolve publication against every compiled destination parent before
    // the first output reservation or attempt file is created. The estimate
    // is deliberately the effective configured ceiling (the lower of the
    // per-attempt and retained-byte limits): admission is advisory, but a run
    // admitted below its own enforced byte ceiling could otherwise fail merely
    // because formatting expands the source representation.
    let mut publication_roots = std::collections::BTreeMap::new();
    for output in pipeline_config.output_configs() {
        if let Some(destinations) = fan_out_destinations.get(&output.name) {
            for (_, destination, _) in destinations {
                insert_publication_root(&mut publication_roots, destination)?;
            }
        } else {
            let destination =
                std::path::PathBuf::from(output.render_runtime_path("<merged>", "<merged>")?);
            insert_publication_root(&mut publication_roots, &destination)?;
        }
    }
    if let Some(dlq) = &pipeline_config.error_handling.dlq {
        if let Some(path) = &dlq.path {
            insert_publication_root(&mut publication_roots, std::path::Path::new(path))?;
        }
        for per_source in dlq.per_source.values() {
            if let Some(path) = &per_source.path {
                insert_publication_root(&mut publication_roots, std::path::Path::new(path))?;
            }
        }
    }
    if publication_roots.is_empty() {
        return Err(PipelineError::Config(
            clinker_plan::config::ConfigError::Validation(
                "compiled pipeline has no file destination roots".to_owned(),
            ),
        ));
    }
    let estimated_attempt_bytes = storage_config
        .publication
        .max_attempt_bytes
        .0
        .min(storage_config.publication.retained_byte_limit.0);
    let mut publication_policy = None;
    for root in publication_roots.values() {
        let observed_free_bytes = clinker_exec::output::attempt::observed_available_space(
            root.as_path(),
        )
        .map_err(|error| {
            PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                error.to_string(),
            ))
        })?;
        let resolved = storage_config
            .publication
            .resolve(root.as_path(), estimated_attempt_bytes, observed_free_bytes)
            .map_err(|error| {
                PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                    error.to_string(),
                ))
            })?;
        if publication_policy
            .as_ref()
            .map(
                |current: &clinker_plan::config::ResolvedPublicationPolicy| {
                    current.explain().observed_free_bytes > observed_free_bytes
                },
            )
            .unwrap_or(true)
        {
            publication_policy = Some(resolved);
        }
    }
    let publication_policy = publication_policy.expect("non-empty roots resolve one policy");
    let created_unix_ms = run_unix_ms()?;
    let eligible_after_unix_ms = created_unix_ms
        .checked_add(
            publication_policy
                .creation_grace_seconds()
                .checked_mul(1_000)
                .ok_or_else(|| {
                    PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                        "storage.publication creation grace overflows the durable clock".to_owned(),
                    ))
                })?,
        )
        .ok_or_else(|| {
            PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                "publication attempt eligibility overflows the durable clock".to_owned(),
            ))
        })?;
    let receipt_root =
        clinker_plan::security::validate_path(std::path::Path::new("."), &workspace_root, false)
            .map_err(|diagnostic| {
                PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                    diagnostic.message,
                ))
            })?;
    let run_attempt =
        clinker_exec::output::attempt::RunAttemptPublication::create_with_root_receipt(
            publication_policy,
            &compiled_plan,
            receipt_root,
            historical_source_identities.into_iter().collect(),
            &execution_id,
            created_unix_ms,
            eligible_after_unix_ms,
            publication_roots.into_values().collect(),
        )
        .map_err(|error| {
            PipelineError::Config(clinker_plan::config::ConfigError::Validation(
                error.to_string(),
            ))
        })?;
    let _attempt_guard = RunAttemptAbandonGuard(run_attempt.clone());
    let output_staging =
        clinker_exec::output::staging::OutputStagingRegistry::for_run_attempt(run_attempt.clone());
    let mut fan_out_writers: std::collections::HashMap<
        String,
        std::collections::HashMap<std::sync::Arc<str>, Box<dyn std::io::Write + Send>>,
    > = std::collections::HashMap::new();
    let mut fan_out_paths: std::collections::HashMap<
        String,
        std::collections::HashMap<std::sync::Arc<str>, String>,
    > = std::collections::HashMap::new();
    for output in pipeline_config.output_configs() {
        // Fan-out path: when the plan flagged this Output for per-
        // source-file routing, render the template once per matched
        // source file. Each rendered path gets its own writer; the
        // dispatcher routes records by `$source.file` Arc.
        if output_is_fan_out(compiled_plan.dag(), &output.name) {
            let mut per_file: std::collections::HashMap<
                std::sync::Arc<str>,
                Box<dyn std::io::Write + Send>,
            > = std::collections::HashMap::new();
            let mut per_file_paths = std::collections::HashMap::new();
            for (file_arc, resolved_path, _source_path) in fan_out_destinations
                .remove(&output.name)
                .unwrap_or_default()
            {
                per_file_paths.insert(
                    std::sync::Arc::clone(&file_arc),
                    resolved_path.to_string_lossy().into_owned(),
                );
                if output.split.is_some() {
                    per_file.insert(file_arc, Box::new(std::io::sink()));
                    continue;
                }
                let bare = resolved_path.clone();
                let unique_suffix_width = output.unique_suffix_width;
                let path_for_n = |n: Option<u64>| -> Result<
                    std::path::PathBuf,
                    clinker_plan::config::ConfigError,
                > {
                    Ok(match n {
                        None => bare.clone(),
                        Some(k) => {
                            let suffix = if unique_suffix_width == 0 {
                                format!("-{k}")
                            } else {
                                format!("-{:0>width$}", k, width = unique_suffix_width as usize)
                            };
                            clinker_exec::output::open::append_suffix_before_ext(&bare, &suffix)
                        }
                    })
                };
                let (_final_path, file) = output_staging.stage_attempt_output(
                    clinker_exec::output::attempt::ArtifactKind::FanOut,
                    output.name.clone(),
                    output.if_exists,
                    false,
                    path_for_n,
                )?;
                per_file.insert(file_arc, Box::new(file));
            }
            fan_out_writers.insert(output.name.clone(), per_file);
            fan_out_paths.insert(output.name.clone(), per_file_paths);
            continue;
        }
        // Non-fan-out split outputs lazily stage each `{seq}` file through the
        // shared ledger inside `build_format_writer`.
        if output.split.is_some() {
            writers.insert(output.name.clone(), Box::new(std::io::sink()));
            continue;
        }
        let bare = std::path::PathBuf::from(output.render_runtime_path("<merged>", "<merged>")?);
        let unique_suffix_width = output.unique_suffix_width;
        let path_for_n =
            |n: Option<u64>| -> Result<std::path::PathBuf, clinker_plan::config::ConfigError> {
                Ok(match n {
                    None => bare.clone(),
                    Some(k) => {
                        let suffix = if unique_suffix_width == 0 {
                            format!("-{k}")
                        } else {
                            format!("-{:0>width$}", k, width = unique_suffix_width as usize)
                        };
                        clinker_exec::output::open::append_suffix_before_ext(&bare, &suffix)
                    }
                })
            };
        let (_final_path, handle) = output_staging.stage_attempt_output(
            clinker_exec::output::attempt::ArtifactKind::Primary,
            output.name.clone(),
            output.if_exists,
            false,
            path_for_n,
        )?;
        let writer: Box<dyn std::io::Write + Send> = Box::new(handle);
        writers.insert(output.name.clone(), writer);
    }

    let registry = clinker_exec::executor::WriterRegistry {
        single: writers,
        fan_out: fan_out_writers,
        fan_out_paths,
        output_staging: output_staging.clone(),
        auto_commit_staged: false,
    };
    // Fresh per-run shutdown token. `ShutdownToken::new()` auto-registers
    // with the process-wide signal-handler registry installed in `main`,
    // so a SIGINT/SIGTERM during the run trips it; the executor polls it
    // at operator chunk boundaries and unwinds gracefully.
    let shutdown_token = machine.map_or_else(
        clinker_exec::pipeline::shutdown::ShutdownToken::new,
        MachineEmitter::shutdown_token,
    );
    let run_params = clinker_exec::executor::PipelineRunParams {
        execution_id: execution_id.clone(),
        batch_id: batch_id.clone(),
        pipeline_vars: effective_runtime_variables.pipeline_vars,
        static_vars: effective_runtime_variables.static_vars,
        source_vars: effective_runtime_variables.source_vars,
        record_vars: effective_runtime_variables.record_vars,
        shutdown_token: Some(shutdown_token.clone()),
        spill_root_dir: spill_root_dir.clone(),
        spill_disk_cap_bytes,
        spill_compress: storage_config.spill.compress,
    };
    // Live run-lifecycle lineage (--lineage-events). Unlike --lineage (a static,
    // plan-only export that exits before reading data), this rides the actual run:
    // build the plan-derived column lineage once from the overlaid `compiled_plan`,
    // open the NDJSON sink, and emit a START now — before the executor runs — so a
    // mid-run crash still leaves an observable open run. The terminal
    // COMPLETE/FAIL/ABORT is emitted at the run boundaries below; the emitter's
    // Drop closes any started-but-unterminated run out as FAIL, covering the
    // early-return output-commit paths between here and the success terminal.
    let mut lineage_emitter: Option<clinker_lineage::LiveRunEmitter<Box<dyn std::io::Write>>> =
        None;
    let mut lineage_started_at: Option<chrono::DateTime<chrono::Utc>> = None;
    if let Some(path) = &args.lineage_events {
        let lineage = clinker_lineage::column_lineage(&compiled_plan, &lineage_base_dir);
        let source_hash = clinker_exec::output::sidecar::hash_to_hex(&pipeline_hash);
        let job =
            clinker_lineage::Job::for_pipeline(pipeline_config.pipeline.name.clone(), source_hash);
        let started_at = chrono::Utc::now();
        let start_time = started_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let writer: Box<dyn std::io::Write> = if path.as_os_str() == std::ffi::OsStr::new("-") {
            // Unlocked stdout handle: locking per write avoids deadlocking the run's
            // own stdout prints (the spill-volume summary and completion line).
            Box::new(std::io::stdout())
        } else {
            Box::new(std::fs::File::create(path).map_err(|e| {
                PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
                    "cannot open --lineage-events output {}: {e}",
                    path.display()
                )))
            })?)
        };
        let mut emitter = clinker_lineage::LiveRunEmitter::new(
            writer,
            lineage,
            job,
            execution_id.clone(),
            start_time,
        );
        emitter.emit_start().map_err(PipelineError::Io)?;
        lineage_started_at = Some(started_at);
        lineage_emitter = Some(emitter);
    }

    // The executor recompiles `compiled_plan.config()` — already the effective,
    // post-overlay config — so the context it recompiles under must NOT carry
    // the overlay ops again (they would double-apply and collide). For a plain
    // run this is identical to `compile_ctx.clone()` (the op stream is empty).
    let machine_progress =
        machine.map(|emitter| emitter.start_execution_progress(shutdown_token.clone()));
    let execution_result = PipelineExecutor::run_plan_with_readers_writers_in_context(
        &compiled_plan,
        readers,
        registry,
        &run_params,
        compile_ctx.without_overlay_ops(),
    );
    let mut machine_control_error = machine_progress.and_then(|worker| worker.finish().err());
    if let Some(error) = &machine_control_error {
        tracing::warn!(error = %error, "machine progress channel failed");
    }
    let mut report = match execution_result {
        Ok(report) => report,
        Err(e) => {
            // Live lineage: the executor failed — close the run out as FAIL with the
            // error message before propagating. Best-effort: a lineage-sink write
            // failure must not mask the real pipeline error.
            if let Some(emitter) = lineage_emitter.as_mut() {
                let now = chrono::Utc::now();
                let duration_ms = lineage_started_at
                    .map(|s| (now - s).num_milliseconds().max(0))
                    .unwrap_or(0);
                let event_time = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                let stats = clinker_lineage::RunStats {
                    duration_ms,
                    ..Default::default()
                };
                if let Err(err) = emitter.emit_terminal(
                    &event_time,
                    clinker_lineage::Terminal::Fail {
                        error: e.to_string(),
                    },
                    stats,
                ) {
                    tracing::warn!(error = %err, "failed to write FAIL lineage event");
                }
            }
            // A failed run keeps its staged copies so the operator can inspect
            // the exact inputs the failure saw (cleanup = on_success); only
            // cleanup = always reaps them on failure.
            source_stager.cleanup(false);
            run_attempt.abandon().map_err(|attempt_error| {
                PipelineError::Io(std::io::Error::other(format!(
                    "pipeline failed and attempt state could not be persisted: {attempt_error}"
                )))
            })?;
            return Err(e);
        }
    };

    if let Some(emitter) = machine
        && let Err(error) = emitter.emit_progress_transition("finalizing")
    {
        tracing::warn!(error = %error, "machine finalization transition failed");
        machine_control_error.get_or_insert(error);
    }
    if machine_control_error.is_some() {
        report.interrupted = true;
    }

    let counters = &report.counters;
    let dlq_entries = &report.dlq_entries;

    // Write DLQ if there are entries and at least one DLQ path is
    // configured (pipeline-wide or per-source). Same atomic
    // temp+rename discipline as primary outputs above — operators
    // inspecting DLQ output should never see a truncated file.
    // Per-source `path:` overrides partition entries into separate
    // sidecar files; entries from sources without an override fall
    // through to `dlq_config.path` (the pipeline-wide sink).
    let publication_preparation = (|| -> Result<(), PipelineError> {
        if report.interrupted {
            return Ok(());
        }
        if !dlq_entries.is_empty()
            && let Some(ref dlq_config) = pipeline_config.error_handling.dlq
        {
            let buckets = clinker_exec::dlq::partition_dlq_entries(dlq_entries, dlq_config);
            if buckets.is_empty() {
                return Ok(());
            }
            let include_reason = dlq_config.include_reason.unwrap_or(true);
            let include_source_row = dlq_config.include_source_row.unwrap_or(true);
            for (target_path, bucket_entries) in &buckets {
                if bucket_entries.is_empty() {
                    continue;
                }
                let bare = target_path.clone();
                let (_final_path, dlq_handle) = output_staging.stage_attempt_output(
                    clinker_exec::output::attempt::ArtifactKind::Dlq,
                    "dead-letter output",
                    clinker_plan::config::IfExistsPolicy::Overwrite,
                    false,
                    move |n| {
                        debug_assert!(n.is_none());
                        Ok(bare.clone())
                    },
                )?;
                let owned: Vec<clinker_exec::executor::DlqEntry> =
                    bucket_entries.iter().map(|e| (*e).clone()).collect();
                clinker_exec::dlq::write_dlq(
                    dlq_handle,
                    &owned,
                    include_reason,
                    include_source_row,
                )
                .map_err(PipelineError::Format)?;
            }
        }
        for output in pipeline_config.output_configs() {
            if !output.write_meta {
                continue;
            }
            let targets = output_staging.pending_paths(&output.name);
            let mut dlq_counts: std::collections::BTreeMap<String, u64> =
                std::collections::BTreeMap::new();
            for entry in dlq_entries {
                if entry.stage.as_deref() == Some(&format!("output:{}", output.name)) {
                    *dlq_counts
                        .entry(format!("{:?}", entry.category))
                        .or_default() += 1;
                }
            }
            let elapsed_ms = (report.finished_at - report.started_at)
                .num_milliseconds()
                .max(0) as u64;
            let hash_full = clinker_exec::output::sidecar::hash_to_hex(&pipeline_hash);
            let hash_short = hash_full[..8.min(hash_full.len())].to_string();
            for target in targets {
                let resolved_path = if output.authored_path_was_absolute() {
                    target.clone()
                } else {
                    let current_dir = std::env::current_dir().map_err(PipelineError::Io)?;
                    target
                        .strip_prefix(current_dir)
                        .unwrap_or(&target)
                        .to_path_buf()
                };
                let sidecar = clinker_exec::output::sidecar::OutputSidecar {
                    pipeline_path: args.config.to_string_lossy().into_owned(),
                    pipeline_hash: hash_full.clone(),
                    pipeline_hash_short: hash_short.clone(),
                    channel: None,
                    clinker_version: env!("CARGO_PKG_VERSION").to_string(),
                    run_started_at: report.started_at.to_rfc3339(),
                    run_finished_at: report.finished_at.to_rfc3339(),
                    elapsed_total_ms: elapsed_ms,
                    execution_id: Some(execution_id.clone()),
                    batch_id: Some(batch_id.clone()),
                    output_name: output.name.clone(),
                    resolved_path: resolved_path.to_string_lossy().into_owned(),
                    record_count: None,
                    bytes_written: None,
                    dlq_counts: dlq_counts.clone(),
                    route_counts: std::collections::BTreeMap::new(),
                    node_timings_ms: std::collections::BTreeMap::new(),
                };
                let bytes = clinker_exec::output::sidecar::serialize_sidecar(&sidecar)?;
                let sidecar_path =
                    clinker_exec::output::sidecar::OutputSidecar::sidecar_path(&target);
                let bare = sidecar_path.clone();
                let (_, mut handle) = output_staging.stage_attempt_output(
                    clinker_exec::output::attempt::ArtifactKind::Sidecar,
                    format!("metadata sidecar for output {:?}", output.name),
                    clinker_plan::config::IfExistsPolicy::Overwrite,
                    false,
                    move |n| {
                        debug_assert!(n.is_none());
                        Ok(bare.clone())
                    },
                )?;
                std::io::Write::write_all(&mut handle, &bytes).map_err(PipelineError::Io)?;
            }
        }
        Ok(())
    })();
    if let Err(error) = publication_preparation {
        source_stager.cleanup(false);
        run_attempt.abandon().map_err(|attempt_error| {
            PipelineError::Io(std::io::Error::other(format!(
                "publication preparation failed and attempt state could not be persisted: {attempt_error}"
            )))
        })?;
        return Err(error);
    }

    let mut publication_failure: Option<String> = None;
    let mut publication_failure_code: Option<&'static str> = None;
    let mut publication_outcome = None;
    if report.interrupted {
        run_attempt.abandon().map_err(|error| {
            PipelineError::Io(std::io::Error::other(format!(
                "interrupted attempt state could not be persisted: {error}"
            )))
        })?;
    } else {
        match run_attempt.mark_all_ready() {
            Err(_) => {
                publication_failure_code = Some("attempt.publication.finalization_failed");
                if run_attempt.abandon().is_err() {
                    publication_failure = Some(publication_failure_diagnostic(
                        &execution_id,
                        PublicationFailureKind::ReadinessAndAbandonment,
                    ));
                } else {
                    publication_failure = Some(publication_failure_diagnostic(
                        &execution_id,
                        PublicationFailureKind::Readiness,
                    ));
                }
            }
            Ok(()) => {
                if let Some(emitter) = machine
                    && let Err(error) = emitter.emit_progress_transition("publishing")
                {
                    tracing::warn!(error = %error, "machine publication transition failed");
                    machine_control_error.get_or_insert(error);
                }
                match run_attempt.publish_run(&output_staging, &shutdown_token) {
                Ok(None) => report.interrupted = true,
                Ok(Some(outcome @ clinker_exec::output::attempt::AttemptPublicationOutcome::Complete {
                    cleanup_debt_count: 0,
                    ..
                })) => publication_outcome = Some(outcome),
                Ok(Some(outcome @ clinker_exec::output::attempt::AttemptPublicationOutcome::Complete {
                    cleanup_debt_count,
                    ..
                })) => {
                    publication_failure_code = Some("attempt.publication.finalization_failed");
                    publication_failure = Some(publication_failure_diagnostic(
                        &execution_id,
                        PublicationFailureKind::CleanupDebt(cleanup_debt_count),
                    ));
                    publication_outcome = Some(outcome);
                }
                Ok(Some(outcome @ clinker_exec::output::attempt::AttemptPublicationOutcome::Incomplete {
                    cleanup_debt_count,
                    ..
                })) => {
                    publication_failure_code = Some("attempt.publication.promotion_failed");
                    publication_failure = Some(publication_failure_diagnostic(
                        &execution_id,
                        PublicationFailureKind::Incomplete(cleanup_debt_count),
                    ));
                    publication_outcome = Some(outcome);
                }
                Err(error) => {
                    publication_failure_code = Some("attempt.publication.promotion_failed");
                    publication_failure = Some(publication_failure_diagnostic(
                        &execution_id,
                        PublicationFailureKind::Publish(publication_error_category(
                            error.source_error(),
                        )),
                    ));
                    publication_outcome = Some(error.outcome().clone());
                }
                }
            }
        }
    }

    if let Some(error) = &publication_failure {
        tracing::error!(execution_id = %execution_id, "{error}");
        eprintln!("{error}");
    } else {
        tracing::info!(
            "Pipeline complete: {} total, {} ok, {} written, {} dlq",
            counters.total_count,
            counters.ok_count,
            counters.records_written,
            counters.dlq_count
        );
    }

    // Per-stage actual spill volume at end-of-run, so an operator can compare
    // each stage's real spilled bytes against the pre-run `--explain` per-stage
    // estimate (the calibration loop #176 exists for). Printed only when a stage
    // actually spilled; a run that stayed in memory adds no noise.
    if machine.is_none() && !report.per_stage_spill_bytes.is_empty() {
        println!("=== Spill Volume (actual, per stage) ===");
        for (stage, bytes) in &report.per_stage_spill_bytes {
            println!("  {stage} → {bytes} bytes");
        }
        println!(
            "  Total: {} bytes (compare against the --explain estimate)",
            report.cumulative_spill_bytes
        );
    }

    // Advisory end-of-run findings — today the per-Output `mapping:` report
    // (W365 / W366). Rendered like the startup storage warnings: to stderr and
    // the tracing log, leaving stdout for the run summary, and never affecting
    // the exit code. Each describes a file that was written and is readable.
    for advisory in &report.advisories {
        tracing::warn!("{advisory}");
        eprintln!("{advisory}");
    }

    // Exit codes per spec §10.2. An interrupted run takes precedence:
    // the pipeline drained what it could before unwinding on the
    // shutdown signal, so report the conventional SIGINT status (130)
    // even when some DLQ entries also landed.
    let exit_code: u8 = if report.interrupted {
        130
    } else if publication_failure.is_some() {
        4
    } else if counters.dlq_count > 0 {
        2
    } else {
        0
    };

    // Live lineage: the run finished — close it out at the true run boundary
    // (outputs persisted, DLQ written). An interrupted drain is an ABORT; every
    // other outcome — including a DLQ-partial success — is a COMPLETE, so the
    // column-lineage facets and the run's final row counts ride the terminal
    // event. Best-effort: a lineage-sink write failure must not fail a run whose
    // data outputs are already committed.
    if let Some(emitter) = lineage_emitter.as_mut() {
        let stats = clinker_lineage::RunStats {
            records_read: counters.total_count,
            records_written: counters.records_written,
            records_dlq: counters.dlq_count,
            duration_ms: (report.finished_at - report.started_at)
                .num_milliseconds()
                .max(0),
        };
        let outcome = if report.interrupted {
            clinker_lineage::Terminal::Abort
        } else if let Some(error) = &publication_failure {
            clinker_lineage::Terminal::Fail {
                error: error.clone(),
            }
        } else {
            clinker_lineage::Terminal::Complete
        };
        let event_time = report
            .finished_at
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        if let Err(err) = emitter.emit_terminal(&event_time, outcome, stats) {
            tracing::warn!(error = %err, "failed to write terminal lineage event");
        }
    }

    // Staging cleanup, keyed on a clean exit. A zero exit code is the
    // "exited cleanly" signal `cleanup = on_success` removes after; an
    // interrupted run (130) or one that produced DLQ entries (2) keeps its
    // staged inputs so the operator can re-run or inspect what the partial run
    // saw. `cleanup = always` reaps regardless; `cleanup = never` keeps the
    // copies as a persistent reuse cache.
    source_stager.cleanup(exit_code == 0);

    // Write execution metrics to spool directory (if configured)
    if let Some(ref dir) = spool_dir {
        let hostname = hostname_string();
        let dlq_path = pipeline_config
            .error_handling
            .dlq
            .as_ref()
            .and_then(|d| d.path.clone());

        let duration_ms = (report.finished_at - report.started_at).num_milliseconds();

        let execution_metrics = ExecutionMetrics {
            execution_id: execution_id.clone(),
            schema_version: 3,
            pipeline_name: pipeline_config.pipeline.name.clone(),
            config_path: args.config.to_string_lossy().into_owned(),
            hostname,
            started_at: report.started_at,
            finished_at: report.finished_at,
            duration_ms,
            exit_code,
            records_total: counters.total_count,
            records_ok: counters.ok_count,
            records_written: counters.records_written,
            records_dlq: counters.dlq_count,
            execution_mode: report.execution_summary.clone(),
            peak_rss_bytes: report.peak_rss_bytes,
            thread_count: num_threads(args),
            input_files: pipeline_config
                .source_configs()
                .map(|i| i.path_str().to_string())
                .collect(),
            output_files: pipeline_config
                .output_configs()
                .map(|o| o.path.clone())
                .collect(),
            dlq_path,
            error: publication_failure.clone(),
            retraction: clinker_exec::metrics::RetractionMetrics::from(&counters.retraction),
            per_source_record_counts: report.per_source_record_counts.clone(),
            per_source_dlq_counts: report.per_source_dlq_counts.clone(),
        };

        if let Err(e) = metrics::write_spool(&execution_metrics, dir) {
            tracing::warn!(
                error = %e,
                spool_dir = %dir.display(),
                execution_id = %execution_metrics.execution_id,
                pipeline_name = %execution_metrics.pipeline_name,
                records_total = execution_metrics.records_total,
                records_ok = execution_metrics.records_ok,
                records_written = execution_metrics.records_written,
                records_dlq = execution_metrics.records_dlq,
                duration_ms = execution_metrics.duration_ms,
                exit_code = execution_metrics.exit_code,
                "metrics spool write failed — emitting inline"
            );
        }
    }

    if let Some(emitter) = machine {
        let terminal_result = if let Some(code) = publication_failure_code {
            let failure = FailureClassification::for_code(code)
                .expect("publication failures use registered codes");
            emitter.emit_failed_with_publication(exit_code, &failure, publication_outcome.as_ref())
        } else {
            emitter.emit_completed_with_publication(exit_code, publication_outcome.as_ref())
        };
        if let Err(error) = terminal_result {
            if publication_failure.is_none() {
                return Err(PipelineError::Io(error));
            }
            tracing::warn!(error = %error, "machine terminal write failed after publication failure");
        }
    }

    Ok(exit_code)
}

fn apply_cli_force_policy(config: &mut clinker_plan::config::PipelineConfig, force: bool) {
    if !force {
        return;
    }
    for node in &mut config.nodes {
        if let clinker_plan::config::PipelineNode::Output { config: body, .. } = &mut node.value
            && body.output.if_exists == clinker_plan::config::IfExistsPolicy::Error
        {
            body.output.if_exists = clinker_plan::config::IfExistsPolicy::Overwrite;
        }
    }
}

/// Whether the named Output is flagged for per-source-file fan-out by
/// the plan-time `populate_fan_out_flags` pass. Returns `false` for
/// outputs whose template lacks per-record tokens or whose input is
/// `Single`-partitioned.
fn output_is_fan_out(
    dag: &clinker_plan::plan::execution::ExecutionPlanDag,
    output_name: &str,
) -> bool {
    use clinker_plan::plan::execution::PlanNode;
    dag.graph
        .node_indices()
        .find(|i| dag.graph[*i].name() == output_name)
        .and_then(|i| match &dag.graph[i] {
            PlanNode::Output { resolved, .. } => {
                resolved.as_ref().map(|r| r.fan_out_per_source_file)
            }
            _ => None,
        })
        .unwrap_or(false)
}

/// Walk back from the named Output through Transform/Sort/Aggregate/
/// Combine nodes to find the FilePartitioned upstream Source that
/// feeds it. For Combine nodes the driver's `$source.file` lineage
/// flows through (each output record derives from a driver record),
/// so we pick whichever parent is FilePartitioned. Returns `None`
/// when the chain runs through a Merge that consumed partitioning.
fn upstream_source_for_output(
    dag: &clinker_plan::plan::execution::ExecutionPlanDag,
    output_name: &str,
) -> Option<String> {
    use clinker_plan::plan::execution::PlanNode;
    use clinker_plan::plan::properties::PartitioningKind;
    let start = dag
        .graph
        .node_indices()
        .find(|i| dag.graph[*i].name() == output_name)?;
    let mut cur = start;
    let mut downstream = None;
    loop {
        match &dag.graph[cur] {
            PlanNode::Source { name, .. } => return Some(name.clone()),
            PlanNode::Merge { .. } => return None,
            PlanNode::Combine { .. } => {
                // Pick the FilePartitioned parent (the driver after
                // the partition propagation pass). Falls back to
                // `None` if the combine destroyed partitioning.
                let parents: Vec<_> = dag
                    .graph
                    .neighbors_undirected(cur)
                    .filter(|neighbor| Some(*neighbor) != downstream)
                    .collect();
                let next = parents.into_iter().find(|p| {
                    dag.node_properties.get(p).is_some_and(|np| {
                        matches!(
                            np.partitioning.kind,
                            PartitioningKind::FilePartitioned { .. }
                        )
                    })
                })?;
                downstream = Some(cur);
                cur = next;
            }
            _ => {
                let next = dag
                    .graph
                    .neighbors_undirected(cur)
                    .find(|neighbor| Some(*neighbor) != downstream)?;
                downstream = Some(cur);
                cur = next;
            }
        }
    }
}

fn run_metrics(cmd: &MetricsCommands) -> Result<(), std::io::Error> {
    match cmd {
        MetricsCommands::Collect(args) => {
            let entries: Vec<_> = metrics::collect_spool(&args.spool_dir)?.collect();
            let count = entries.len();

            if args.dry_run {
                println!(
                    "Would collect {count} file(s) from {}",
                    args.spool_dir.display()
                );
                for entry in &entries {
                    println!(
                        "  {} ({})",
                        entry.path.display(),
                        entry.metrics.pipeline_name
                    );
                }
                return Ok(());
            }

            let mut written = 0usize;
            for entry in entries {
                metrics::append_ndjson(&entry.metrics, &args.output_file)?;
                if args.delete_after_collect
                    && let Err(e) = std::fs::remove_file(&entry.path)
                {
                    tracing::warn!(
                        path = %entry.path.display(),
                        error = %e,
                        "metrics collect: failed to delete spool file after collection"
                    );
                }
                written += 1;
            }

            println!(
                "Collected {written} file(s) → {}",
                args.output_file.display()
            );
            Ok(())
        }
    }
}

/// Resolve thread count from CLI args or default to `num_cpus`.
fn num_threads(args: &RunArgs) -> usize {
    args.threads.unwrap_or_else(num_cpus::get)
}

/// Render a "Resolved Outputs" block listing each output's expanded
/// path, collision policy, and sidecar opt-in. Called after path
/// templates resolve, before --explain or --dry-run early-returns.
///
/// `{n}` is shown literally (not expanded) when the policy is
/// `unique_suffix` so the user can see where the collision counter
/// would land at runtime.
fn print_resolved_outputs(config: &clinker_plan::config::PipelineConfig) {
    use clinker_plan::config::IfExistsPolicy;
    println!("=== Resolved Outputs ===");
    println!();
    for output in config.output_configs() {
        let policy = match output.if_exists {
            IfExistsPolicy::Overwrite => "overwrite",
            IfExistsPolicy::Error => "error",
            IfExistsPolicy::UniqueSuffix => "unique_suffix",
        };
        let split_note = match &output.split {
            Some(s) => format!(" (split, naming={:?})", s.naming),
            None => String::new(),
        };
        let unique_note = if matches!(output.if_exists, IfExistsPolicy::UniqueSuffix) {
            let width = output.unique_suffix_width;
            if width == 0 {
                " — collisions append `-{n}` before extension".to_string()
            } else {
                format!(" — collisions append `-{{n:0{width}}}` before extension")
            }
        } else {
            String::new()
        };
        println!("  '{}' → {}{}", output.name, output.path, split_note,);
        println!(
            "      [if_exists={policy}, write_meta={}]{unique_note}",
            output.write_meta,
        );
    }
    println!();
}

/// Render the cap-headroom line for `clinker run --explain`.
///
/// Reports the spill cap minus the run's estimated spill volume, plus a
/// per-invocation disclaimer. Returns an empty string when no
/// `storage.spill.disk_cap_bytes` is configured (unlimited spill has no
/// headroom to report) or the estimate is unknown (`0`). The figure is **per
/// invocation**: under the partition-and-run model several `clinker`
/// invocations can share one spill volume and one cap, so the disclaimer states
/// the headroom does not account for sibling invocations sharing the volume.
/// Rendered in raw bytes, matching the "Spill disk cap" line above it.
fn cap_headroom_explain(disk_cap_bytes: Option<u64>, estimated_spill_bytes: u64) -> String {
    let Some(cap) = disk_cap_bytes else {
        return String::new();
    };
    if estimated_spill_bytes == 0 {
        return String::new();
    }
    let headroom = cap.saturating_sub(estimated_spill_bytes);
    let pct = if cap == 0 {
        0.0
    } else {
        (estimated_spill_bytes as f64 / cap as f64) * 100.0
    };
    let over_threshold = estimated_spill_bytes as f64 >= cap as f64 * 0.80;
    let mut out = format!(
        "Cap headroom: {headroom} bytes free ({estimated_spill_bytes} estimated of {cap} cap, \
         {pct:.0}%) [per invocation — does NOT account for sibling invocations sharing the \
         spill volume under partition-and-run]\n",
    );
    if over_threshold {
        out.push_str(
            "  WARNING: the estimate exceeds 80% of the cap; a real run may abort with a \
             spill-cap error (E320). Raise storage.spill.disk_cap_bytes or reduce the spill \
             footprint.\n",
        );
    }
    out
}

/// Render the `=== Staging Plan ===` block for `clinker run --explain`.
///
/// For each file-backed source, resolves its matcher to the file set the run
/// would read and emits one line per file: whether it would be staged, the
/// resolved `<staging_root>/<source_id>.staged` path, and (under
/// `on_existing = reuse`) the reuse-if-fresh cache decision (hit/miss). Network
/// sources are not stagable and render an explicit in-place note. When staging
/// is disabled the block states that every source reads in place. Read-only:
/// resolves through the same [`clinker_channel::SourceStager::plan_entry`] the
/// run would consult, copying nothing.
fn staging_plan_explain(
    config: &clinker_plan::config::PipelineConfig,
    staging_policy: &clinker_plan::config::StagingPolicy,
    discovery_anchor: &std::path::Path,
) -> String {
    let mut out = String::from("=== Staging Plan ===\n\n");
    if !staging_policy.enabled {
        out.push_str("Source staging is disabled — every source reads in place.\n\n");
        return out;
    }
    let stager = clinker_channel::SourceStager::new(staging_policy.clone());
    for body in config.source_bodies() {
        let source = &body.source;
        if !source.transport.is_file() {
            out.push_str(&format!(
                "Source '{}': not stagable (network source reads in place)\n",
                source.name
            ));
            continue;
        }
        out.push_str(&format!("Source '{}':\n", source.name));
        // Resolve the matcher to its file set with the same anchor the run
        // uses. A discovery failure (no match, bad glob) is reported inline
        // rather than aborting the explain; the run's own discovery will
        // surface the coded diagnostic.
        match clinker_plan::config::discovery::discover(source, discovery_anchor) {
            Ok(outcome) => {
                let files = outcome.files();
                if files.is_empty() {
                    out.push_str("  (no files matched)\n");
                }
                for f in files {
                    let entry = stager.plan_entry(&f.path);
                    if entry.staged {
                        let path = entry
                            .staged_path
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_default();
                        out.push_str(&format!(
                            "  {} → staged: yes, path: {}, reuse: {}\n",
                            f.path.display(),
                            path,
                            entry.reuse.label(),
                        ));
                    } else {
                        out.push_str(&format!(
                            "  {} → staged: no (no pattern match, reads in place)\n",
                            f.path.display(),
                        ));
                    }
                }
            }
            Err(e) => {
                out.push_str(&format!("  (discovery failed: {e})\n"));
            }
        }
    }
    out.push('\n');
    out
}

/// Assemble the structured storage observability summary for
/// `clinker run --explain --format json`.
///
/// Carries the same information the text path renders — per-stage spill
/// estimate, resolved spill root, spill disk cap, per-operator spill
/// compression, cap headroom, and the per-source staging plan — but
/// structured so downstream tooling reads per-stage figures and the cap /
/// staging summary without re-parsing prose. The plan-derivable parts come
/// from the DAG ([`estimated_spill_json`](clinker_plan::plan::execution::ExecutionPlanDag::estimated_spill_json)
/// / [`spill_compression_json`](clinker_plan::plan::execution::ExecutionPlanDag::spill_compression_json)),
/// so they cannot drift from the text rendering; the resolved storage
/// config the CLI loaded supplies the spill root / cap / compression /
/// staging. `config_path` is the pipeline file's path: its parent is the
/// discovery anchor the run uses, so the staged paths shown match the
/// paths the real run would write.
fn build_storage_summary_json(
    dag: &clinker_plan::plan::execution::ExecutionPlanDag,
    config: &clinker_plan::config::PipelineConfig,
    storage: &clinker_plan::config::StorageConfig,
    spill_root_dir: Option<&std::path::Path>,
    config_path: &std::path::Path,
) -> clinker_plan::plan::execution::StorageSummaryJson {
    use clinker_plan::plan::execution::{
        CapHeadroomJson, SpillRootJson, StagingFileJson, StagingPlanJson, StagingSourceJson,
        StorageSummaryJson,
    };

    let spill_disk_cap_bytes = storage.spill.disk_cap();
    let compress = storage.spill.compress;
    let staging_policy = &storage.staging;
    let batch_size = config
        .pipeline
        .batch_size
        .unwrap_or(clinker_exec::executor::DEFAULT_BATCH_SIZE);
    let discovery_anchor = config_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let discovery_anchor = discovery_anchor.as_path();

    // Spill root: configured dir, else the OS temp dir — the same
    // resolution the text path's "Spill root" line reports.
    let spill_root = match spill_root_dir {
        Some(dir) => SpillRootJson {
            path: dir.display().to_string(),
            source: "storage.spill.dir".to_string(),
        },
        None => SpillRootJson {
            path: std::env::temp_dir().display().to_string(),
            source: "OS temp dir (default)".to_string(),
        },
    };

    // Cap headroom: cap minus the run's estimated spill volume. Omitted
    // when no cap is configured or the estimate is unknown (`0`), matching
    // the text path's `cap_headroom_explain` suppression.
    let estimated_spill_bytes = dag.estimated_spill_bytes();
    let cap_headroom = match spill_disk_cap_bytes {
        Some(cap) if estimated_spill_bytes > 0 => {
            let headroom_bytes = cap.saturating_sub(estimated_spill_bytes);
            let pct_of_cap = if cap == 0 {
                0.0
            } else {
                (estimated_spill_bytes as f64 / cap as f64) * 100.0
            };
            let over_threshold = estimated_spill_bytes as f64 >= cap as f64 * 0.80;
            Some(CapHeadroomJson {
                headroom_bytes,
                estimated_bytes: estimated_spill_bytes,
                cap_bytes: cap,
                pct_of_cap,
                over_threshold,
            })
        }
        _ => None,
    };

    // Staging plan: resolve each source's matcher through the same
    // `SourceStager::plan_entry` the text path consults, copying nothing.
    let staging = if !staging_policy.enabled {
        StagingPlanJson {
            enabled: false,
            sources: Vec::new(),
        }
    } else {
        let stager = clinker_channel::SourceStager::new(staging_policy.clone());
        let mut sources = Vec::new();
        for body in config.source_bodies() {
            let source = &body.source;
            if !source.transport.is_file() {
                sources.push(StagingSourceJson {
                    name: source.name.clone(),
                    stagable: false,
                    files: Vec::new(),
                    discovery_error: None,
                });
                continue;
            }
            match clinker_plan::config::discovery::discover(source, discovery_anchor) {
                Ok(outcome) => {
                    let files = outcome
                        .files()
                        .iter()
                        .map(|f| {
                            let entry = stager.plan_entry(&f.path);
                            if entry.staged {
                                StagingFileJson {
                                    source_path: f.path.display().to_string(),
                                    staged: true,
                                    staged_path: entry
                                        .staged_path
                                        .as_ref()
                                        .map(|p| p.display().to_string()),
                                    reuse: Some(entry.reuse.label().to_string()),
                                }
                            } else {
                                StagingFileJson {
                                    source_path: f.path.display().to_string(),
                                    staged: false,
                                    staged_path: None,
                                    reuse: None,
                                }
                            }
                        })
                        .collect();
                    sources.push(StagingSourceJson {
                        name: source.name.clone(),
                        stagable: true,
                        files,
                        discovery_error: None,
                    });
                }
                Err(e) => sources.push(StagingSourceJson {
                    name: source.name.clone(),
                    stagable: true,
                    files: Vec::new(),
                    discovery_error: Some(e.to_string()),
                }),
            }
        }
        StagingPlanJson {
            enabled: true,
            sources,
        }
    };

    StorageSummaryJson {
        spill_root,
        spill_disk_cap_bytes,
        estimated_spill: dag.estimated_spill_json(),
        spill_compression: dag.spill_compression_json(compress, batch_size),
        cap_headroom,
        staging,
    }
}

/// Best-effort hostname for the metrics payload.
fn hostname_string() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| {
            // Read from /etc/hostname on Linux
            std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|_| "unknown".to_string())
}

#[derive(Debug)]
struct AttemptCommandError(String);

impl AttemptCommandError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for AttemptCommandError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for AttemptCommandError {}

struct AttemptCommandContext {
    query: Option<clinker_exec::output::attempt::AttemptQuery>,
    root_ids: Vec<String>,
    workspace_root: PathBuf,
    pipeline: String,
    identity_argv: Vec<String>,
}

#[derive(Serialize)]
struct AttemptOperationView {
    operation: &'static str,
    pipeline: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<&'static str>,
    roots: Vec<AttemptRootView>,
}

#[derive(Serialize)]
struct AttemptRootView {
    root_id: String,
    disposition: String,
    attempts: Vec<AttemptInspectionView>,
    selected_execution_ids: Vec<String>,
    removed_execution_ids: Vec<String>,
    kept_execution_ids: Vec<String>,
    removed_artifact_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    continuation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resume_command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resume_argv: Option<Vec<String>>,
    cleanup_debt: Vec<AttemptDebtView>,
    diagnostics: Vec<AttemptDiagnosticView>,
    bounds: AttemptBoundsView,
}

#[derive(Serialize)]
struct AttemptInspectionView {
    execution_id: String,
    disposition: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    eligible_after_unix_ms: Option<u64>,
    artifact_ids: Vec<String>,
    eligible: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    cleanup_debt: Vec<AttemptDebtView>,
    diagnostics: Vec<AttemptDiagnosticView>,
    bounds: AttemptBoundsView,
}

#[derive(Serialize)]
struct AttemptDebtView {
    kind: &'static str,
    detail: &'static str,
}

#[derive(Serialize)]
struct AttemptDiagnosticView {
    diagnostic_code: &'static str,
    failure_code: &'static str,
    failure_category: &'static str,
    operation: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact_id: Option<String>,
    final_visibility: &'static str,
    durability_uncertain: bool,
    retry_advice: &'static str,
    recovery_command: String,
    recovery_argv: Vec<String>,
}

#[derive(Clone, Copy, Serialize)]
struct AttemptBoundsView {
    considered_entries: u64,
    considered_bytes: u64,
    elapsed_ms: u64,
}

impl From<clinker_exec::output::attempt::AttemptQueryBounds> for AttemptBoundsView {
    fn from(bounds: clinker_exec::output::attempt::AttemptQueryBounds) -> Self {
        Self {
            considered_entries: bounds.considered_entries(),
            considered_bytes: bounds.considered_bytes(),
            elapsed_ms: bounds.elapsed_ms(),
        }
    }
}

fn run_attempts(command: &AttemptsCommands) -> Result<u8, AttemptCommandError> {
    if matches!(command, AttemptsCommands::Purge(args) if args.execute) {
        clinker_exec::pipeline::shutdown::install_signal_handler().map_err(|error| {
            AttemptCommandError::new(format!("cannot install cleanup signal handler: {error}"))
        })?;
    }
    match command {
        AttemptsCommands::List(args) => run_attempts_list(args),
        AttemptsCommands::Inspect(args) => run_attempts_inspect(args),
        AttemptsCommands::Purge(args) => run_attempts_purge(args),
    }
}

fn run_attempts_list(args: &AttemptsListArgs) -> Result<u8, AttemptCommandError> {
    if let Some(execution_id) = args.identity.path_execution_id.as_deref() {
        validate_execution_selector(execution_id)?;
    }
    let context = compile_attempt_context(
        &args.pipeline,
        &args.identity,
        args.identity.path_execution_id.as_deref(),
    )?;
    let continuation = decode_attempt_continuation(args.continuation.as_deref())?;
    let observed_unix_ms = observed_unix_ms()?;
    let mut roots = Vec::new();
    let Some(query) = context.query.as_ref() else {
        if continuation.is_some() {
            return Err(AttemptCommandError::new(
                "--continuation cannot be used because the compiled pipeline has no existing owned roots",
            ));
        }
        render_attempt_operation(
            AttemptOperationView {
                operation: "list",
                pipeline: context.pipeline,
                mode: None,
                roots,
            },
            args.format,
        )?;
        return Ok(0);
    };

    if let Some(continuation) = continuation.as_ref() {
        let mut last_error = None;
        for root_id in &context.root_ids {
            match query.list(root_id, observed_unix_ms, Some(continuation)) {
                Ok(list) => {
                    roots.push(list_root_view(root_id, &list, args.show_paths, &context)?);
                    last_error = None;
                    break;
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        if let Some(error) = last_error {
            return Err(AttemptCommandError::new(format!(
                "invalid --continuation: {error}"
            )));
        }
    } else {
        for root_id in &context.root_ids {
            let list = query
                .list(root_id, observed_unix_ms, None)
                .map_err(attempt_query_error)?;
            roots.push(list_root_view(root_id, &list, args.show_paths, &context)?);
        }
    }
    let has_debt = roots.iter().any(root_has_debt);
    render_attempt_operation(
        AttemptOperationView {
            operation: "list",
            pipeline: context.pipeline,
            mode: None,
            roots,
        },
        args.format,
    )?;
    Ok(if has_debt { 4 } else { 0 })
}

fn run_attempts_inspect(args: &AttemptsInspectArgs) -> Result<u8, AttemptCommandError> {
    validate_execution_selector(&args.execution_id)?;
    if let Some(execution_id) = args.identity.path_execution_id.as_deref() {
        validate_execution_selector(execution_id)?;
    }
    let context = compile_attempt_context(
        &args.pipeline,
        &args.identity,
        args.identity
            .path_execution_id
            .as_deref()
            .or(Some(args.execution_id.as_str())),
    )?;
    let observed_unix_ms = observed_unix_ms()?;
    let mut roots = Vec::new();
    if let Some(query) = context.query.as_ref() {
        for root_id in &context.root_ids {
            let inspection = query
                .inspect(root_id, &args.execution_id, observed_unix_ms)
                .map_err(attempt_query_error)?;
            let bounds = inspection.bounds().into();
            let cleanup_debt = debt_views(inspection.cleanup_debt());
            let diagnostics = inspection_diagnostic_views(
                &inspection,
                clinker_core_types::diagnostic::AttemptOperation::Inspect,
                &context,
            );
            roots.push(AttemptRootView {
                root_id: root_id.clone(),
                disposition: cleanup_disposition_name(inspection.disposition()).to_owned(),
                attempts: vec![inspection_view(
                    &inspection,
                    args.show_paths,
                    &context,
                    clinker_core_types::diagnostic::AttemptOperation::Inspect,
                )],
                selected_execution_ids: Vec::new(),
                removed_execution_ids: Vec::new(),
                kept_execution_ids: Vec::new(),
                removed_artifact_count: 0,
                continuation: None,
                resume_command: None,
                resume_argv: None,
                cleanup_debt,
                diagnostics,
                bounds,
            });
        }
    }
    let has_debt = roots.iter().any(root_has_debt);
    render_attempt_operation(
        AttemptOperationView {
            operation: "inspect",
            pipeline: context.pipeline,
            mode: None,
            roots,
        },
        args.format,
    )?;
    Ok(if has_debt { 4 } else { 0 })
}

fn run_attempts_purge(args: &AttemptsPurgeArgs) -> Result<u8, AttemptCommandError> {
    match (&args.execution_id, args.expired) {
        (Some(_), true) | (None, false) => {
            return Err(AttemptCommandError::new(
                "purge requires exactly one selector: --execution-id <id> or --expired",
            ));
        }
        (Some(execution_id), false) => validate_execution_selector(execution_id)?,
        (None, true) => {}
    }
    if let Some(execution_id) = args.identity.path_execution_id.as_deref() {
        validate_execution_selector(execution_id)?;
    }
    let context = compile_attempt_context(
        &args.pipeline,
        &args.identity,
        args.identity
            .path_execution_id
            .as_deref()
            .or(args.execution_id.as_deref()),
    )?;
    let continuation = decode_attempt_continuation(args.continuation.as_deref())?;
    let observed_unix_ms = observed_unix_ms()?;
    let mut roots = Vec::new();
    let Some(query) = context.query.as_ref() else {
        if continuation.is_some() {
            return Err(AttemptCommandError::new(
                "--continuation cannot be used because the compiled pipeline has no existing owned roots",
            ));
        }
        render_attempt_operation(
            AttemptOperationView {
                operation: "purge",
                pipeline: context.pipeline,
                mode: Some(if args.execute { "execute" } else { "preview" }),
                roots,
            },
            args.format,
        )?;
        return Ok(0);
    };

    let shutdown = clinker_exec::pipeline::shutdown::ShutdownToken::new();
    if let Some(continuation) = continuation.as_ref() {
        let mut last_error = None;
        for root_id in &context.root_ids {
            match purge_root_view(
                query,
                root_id,
                args,
                observed_unix_ms,
                Some(continuation),
                &shutdown,
                &context,
            ) {
                Ok(root) => {
                    roots.push(root);
                    last_error = None;
                    break;
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        if let Some(error) = last_error {
            return Err(AttemptCommandError::new(format!(
                "invalid --continuation: {error}"
            )));
        }
    } else {
        for root_id in &context.root_ids {
            roots.push(purge_root_view(
                query,
                root_id,
                args,
                observed_unix_ms,
                None,
                &shutdown,
                &context,
            )?);
        }
    }
    let has_debt = roots.iter().any(root_has_debt);
    render_attempt_operation(
        AttemptOperationView {
            operation: "purge",
            pipeline: context.pipeline,
            mode: Some(if args.execute { "execute" } else { "preview" }),
            roots,
        },
        args.format,
    )?;
    Ok(if has_debt { 4 } else { 0 })
}

#[allow(clippy::too_many_arguments)]
fn purge_root_view(
    query: &clinker_exec::output::attempt::AttemptQuery,
    root_id: &str,
    args: &AttemptsPurgeArgs,
    observed_unix_ms: u64,
    continuation: Option<&clinker_exec::output::attempt::AttemptContinuation>,
    shutdown: &clinker_exec::pipeline::shutdown::ShutdownToken,
    context: &AttemptCommandContext,
) -> Result<AttemptRootView, AttemptCommandError> {
    let request = match args.execution_id.as_deref() {
        Some(execution_id) => query.purge_execution(root_id, execution_id),
        None => query.purge_expired(root_id),
    }
    .map_err(attempt_query_error)?;

    if args.execute {
        let report = query
            .execute(&request, observed_unix_ms, continuation, shutdown)
            .map_err(attempt_query_error)?;
        let diagnostics = root_diagnostic_views(
            report.cleanup_debt(),
            clinker_core_types::diagnostic::AttemptOperation::Purge,
            context,
        );
        let continuation = encode_attempt_continuation(report.continuation())?;
        let resume_command = continuation
            .as_deref()
            .map(|continuation| purge_resume_command(args, &context.pipeline, continuation));
        let resume_argv = continuation
            .as_deref()
            .map(|continuation| purge_resume_argv(args, &context.pipeline, continuation));
        Ok(AttemptRootView {
            root_id: root_id.to_owned(),
            disposition: purge_disposition_name(report.disposition()).to_owned(),
            attempts: Vec::new(),
            selected_execution_ids: report.selected_execution_ids().to_vec(),
            removed_execution_ids: report.removed_execution_ids().to_vec(),
            kept_execution_ids: report.kept_execution_ids().to_vec(),
            removed_artifact_count: report.removed_artifact_count(),
            continuation,
            resume_command,
            resume_argv,
            cleanup_debt: debt_views(report.cleanup_debt()),
            diagnostics,
            bounds: report.bounds().into(),
        })
    } else {
        let preview = query
            .preview(&request, observed_unix_ms, continuation)
            .map_err(attempt_query_error)?;
        let attempts = preview
            .inspections()
            .iter()
            .map(|inspection| {
                inspection_view(
                    inspection,
                    args.show_paths,
                    context,
                    clinker_core_types::diagnostic::AttemptOperation::Purge,
                )
            })
            .collect::<Vec<_>>();
        let mut diagnostics = preview
            .inspections()
            .iter()
            .flat_map(|inspection| {
                inspection_diagnostic_views(
                    inspection,
                    clinker_core_types::diagnostic::AttemptOperation::Purge,
                    context,
                )
            })
            .collect::<Vec<_>>();
        diagnostics.extend(root_diagnostic_views(
            preview.cleanup_debt(),
            clinker_core_types::diagnostic::AttemptOperation::Purge,
            context,
        ));
        let kept_execution_ids = preview
            .inspections()
            .iter()
            .map(|inspection| inspection.execution_id().to_owned())
            .collect();
        let continuation = encode_attempt_continuation(preview.continuation())?;
        let resume_command = continuation
            .as_deref()
            .map(|continuation| purge_resume_command(args, &context.pipeline, continuation));
        let resume_argv = continuation
            .as_deref()
            .map(|continuation| purge_resume_argv(args, &context.pipeline, continuation));
        Ok(AttemptRootView {
            root_id: root_id.to_owned(),
            disposition: "preview".to_owned(),
            attempts,
            selected_execution_ids: preview.selected_execution_ids().to_vec(),
            removed_execution_ids: Vec::new(),
            kept_execution_ids,
            removed_artifact_count: 0,
            continuation,
            resume_command,
            resume_argv,
            cleanup_debt: debt_views(preview.cleanup_debt()),
            diagnostics,
            bounds: preview.bounds().into(),
        })
    }
}

fn list_root_view(
    root_id: &str,
    list: &clinker_exec::output::attempt::AttemptList,
    show_paths: bool,
    context: &AttemptCommandContext,
) -> Result<AttemptRootView, AttemptCommandError> {
    let attempts = list
        .entries()
        .iter()
        .map(|entry| {
            inspection_view(
                entry.inspection(),
                show_paths,
                context,
                clinker_core_types::diagnostic::AttemptOperation::List,
            )
        })
        .collect::<Vec<_>>();
    let mut diagnostics = list
        .entries()
        .iter()
        .flat_map(|entry| {
            inspection_diagnostic_views(
                entry.inspection(),
                clinker_core_types::diagnostic::AttemptOperation::List,
                context,
            )
        })
        .collect::<Vec<_>>();
    diagnostics.extend(root_diagnostic_views(
        list.cleanup_debt(),
        clinker_core_types::diagnostic::AttemptOperation::List,
        context,
    ));
    let continuation = encode_attempt_continuation(list.continuation())?;
    let resume_argv = continuation.as_deref().map(|continuation| {
        let mut argv = vec![
            "clinker".to_owned(),
            "attempts".to_owned(),
            "list".to_owned(),
            context.pipeline.clone(),
        ];
        argv.extend(context.identity_argv.iter().cloned());
        argv.extend(["--continuation".to_owned(), continuation.to_owned()]);
        argv
    });
    let resume_command = resume_argv.as_deref().map(render_attempt_command);
    Ok(AttemptRootView {
        root_id: root_id.to_owned(),
        disposition: "listed".to_owned(),
        attempts,
        selected_execution_ids: Vec::new(),
        removed_execution_ids: Vec::new(),
        kept_execution_ids: Vec::new(),
        removed_artifact_count: 0,
        continuation,
        resume_command,
        resume_argv,
        cleanup_debt: debt_views(list.cleanup_debt()),
        diagnostics,
        bounds: AttemptBoundsView {
            considered_entries: list.considered_entries(),
            considered_bytes: list.considered_bytes(),
            elapsed_ms: list.elapsed_ms(),
        },
    })
}

fn inspection_view(
    inspection: &clinker_exec::output::attempt::AttemptInspection,
    show_paths: bool,
    context: &AttemptCommandContext,
    operation: clinker_core_types::diagnostic::AttemptOperation,
) -> AttemptInspectionView {
    let path = show_paths
        .then(|| {
            inspection.physical_path_for_sanitized_output(
                clinker_exec::output::attempt::SanitizedPathOptIn,
            )
        })
        .flatten()
        .map(|path| sanitize_attempt_path(path, &context.workspace_root));
    AttemptInspectionView {
        execution_id: inspection.execution_id().to_owned(),
        disposition: cleanup_disposition_name(inspection.disposition()),
        state: inspection.state().map(attempt_state_name),
        created_unix_ms: inspection.created_unix_ms(),
        eligible_after_unix_ms: inspection.eligible_after_unix_ms(),
        artifact_ids: inspection.artifact_ids().to_vec(),
        eligible: inspection.is_eligible(),
        path,
        cleanup_debt: debt_views(inspection.cleanup_debt()),
        diagnostics: inspection_diagnostic_views(inspection, operation, context),
        bounds: inspection.bounds().into(),
    }
}

fn inspection_diagnostic_views(
    inspection: &clinker_exec::output::attempt::AttemptInspection,
    operation: clinker_core_types::diagnostic::AttemptOperation,
    context: &AttemptCommandContext,
) -> Vec<AttemptDiagnosticView> {
    let (artifact_id, final_visibility, durability_uncertain) =
        diagnostic_artifact_evidence(inspection.artifact_states());
    inspection
        .cleanup_debt()
        .iter()
        .filter_map(|debt| {
            clinker_core_types::diagnostic::AttemptDiagnosticData::for_failure(
                failure_code_for_debt(debt.kind()),
                operation,
                inspection.execution_id(),
                artifact_id,
                final_visibility,
                durability_uncertain,
                &context.pipeline,
            )
        })
        .map(|diagnostic| {
            let mut recovery_argv = diagnostic.recovery_argv().to_vec();
            recovery_argv.extend(context.identity_argv.iter().cloned());
            AttemptDiagnosticView {
                diagnostic_code: diagnostic.diagnostic_code(),
                failure_code: diagnostic.failure_code(),
                failure_category: diagnostic.failure_category().as_str(),
                operation: diagnostic.operation().as_str(),
                execution_id: Some(diagnostic.execution_id().to_owned()),
                artifact_id: diagnostic.artifact_id().map(str::to_owned),
                final_visibility: diagnostic.final_visibility().as_str(),
                durability_uncertain: diagnostic.durability_uncertain(),
                retry_advice: diagnostic.retry_advice().as_str(),
                recovery_command: render_attempt_command(&recovery_argv),
                recovery_argv,
            }
        })
        .collect()
}

fn diagnostic_artifact_evidence(
    artifact_states: &[(String, clinker_exec::output::attempt::ArtifactState)],
) -> (
    Option<&str>,
    clinker_core_types::diagnostic::FinalVisibility,
    bool,
) {
    use clinker_exec::output::attempt::ArtifactState;

    if let Some((artifact_id, _)) = artifact_states
        .iter()
        .find(|(_, state)| *state == ArtifactState::VisibleUnsynchronized)
    {
        return (
            Some(artifact_id),
            clinker_core_types::diagnostic::FinalVisibility::Some,
            true,
        );
    }
    if let Some((artifact_id, _)) = artifact_states
        .iter()
        .find(|(_, state)| *state == ArtifactState::Promoting)
    {
        return (
            Some(artifact_id),
            clinker_core_types::diagnostic::FinalVisibility::Unknown,
            true,
        );
    }
    if let Some((artifact_id, _)) = artifact_states
        .iter()
        .find(|(_, state)| *state == ArtifactState::Published)
    {
        return (
            Some(artifact_id),
            clinker_core_types::diagnostic::FinalVisibility::Some,
            false,
        );
    }
    (
        artifact_states
            .first()
            .map(|(artifact_id, _)| artifact_id.as_str()),
        clinker_core_types::diagnostic::FinalVisibility::None,
        false,
    )
}

fn root_diagnostic_views(
    debt: &[clinker_exec::output::attempt::CleanupDebt],
    operation: clinker_core_types::diagnostic::AttemptOperation,
    context: &AttemptCommandContext,
) -> Vec<AttemptDiagnosticView> {
    debt.iter()
        .filter_map(|debt| {
            let failure_code = failure_code_for_debt(debt.kind());
            let failure = clinker_core_types::FailureClassification::for_code(failure_code)?;
            let diagnostic_code = match debt.kind() {
                clinker_exec::output::attempt::CleanupDebtKind::EntryBudget
                | clinker_exec::output::attempt::CleanupDebtKind::ByteBudget
                | clinker_exec::output::attempt::CleanupDebtKind::TimeBudget => "E372",
                _ => "E371",
            };
            let mut recovery_argv = vec![
                "clinker".to_owned(),
                "attempts".to_owned(),
                "list".to_owned(),
                context.pipeline.clone(),
            ];
            recovery_argv.extend(context.identity_argv.iter().cloned());
            Some(AttemptDiagnosticView {
                diagnostic_code,
                failure_code: failure.code(),
                failure_category: failure.category().as_str(),
                operation: operation.as_str(),
                execution_id: None,
                artifact_id: None,
                final_visibility: clinker_core_types::diagnostic::FinalVisibility::Unknown.as_str(),
                durability_uncertain: true,
                retry_advice: failure.retry_advice().as_str(),
                recovery_command: render_attempt_command(&recovery_argv),
                recovery_argv,
            })
        })
        .collect()
}

fn debt_views(debt: &[clinker_exec::output::attempt::CleanupDebt]) -> Vec<AttemptDebtView> {
    debt.iter()
        .map(|debt| AttemptDebtView {
            kind: cleanup_debt_kind_name(debt.kind()),
            detail: debt.detail(),
        })
        .collect()
}

fn render_attempt_operation(
    view: AttemptOperationView,
    format: AttemptsFormat,
) -> Result<(), AttemptCommandError> {
    match format {
        AttemptsFormat::Json => {
            let json = serde_json::to_string(&view).map_err(|error| {
                AttemptCommandError::new(format!("cannot encode attempt result: {error}"))
            })?;
            println!("{json}");
        }
        AttemptsFormat::Text => render_attempt_text(&view),
    }
    Ok(())
}

fn render_attempt_text(view: &AttemptOperationView) {
    println!("operation: {}", view.operation);
    println!("pipeline: {}", view.pipeline);
    if let Some(mode) = view.mode {
        println!("mode: {mode}");
    }
    if view.roots.is_empty() {
        println!("attempts: none");
        return;
    }
    for root in &view.roots {
        println!("root: {}", root.root_id);
        println!("  disposition: {}", root.disposition);
        for attempt in &root.attempts {
            println!("  execution: {}", attempt.execution_id);
            println!("    disposition: {}", attempt.disposition);
            println!("    state: {}", attempt.state.unwrap_or("absent"));
            println!("    eligible: {}", attempt.eligible);
            println!("    artifacts: {}", attempt.artifact_ids.len());
            for artifact_id in &attempt.artifact_ids {
                println!("      - {artifact_id}");
            }
            if let Some(path) = &attempt.path {
                println!("    path: {path}");
            }
            for diagnostic in &attempt.diagnostics {
                render_attempt_diagnostic_text(diagnostic, "    ");
            }
        }
        if !root.selected_execution_ids.is_empty() {
            println!("  selected: {}", root.selected_execution_ids.join(","));
        }
        if !root.removed_execution_ids.is_empty() {
            println!("  removed: {}", root.removed_execution_ids.join(","));
        }
        if !root.kept_execution_ids.is_empty() {
            println!("  kept: {}", root.kept_execution_ids.join(","));
        }
        println!("  removed_artifacts: {}", root.removed_artifact_count);
        for debt in &root.cleanup_debt {
            println!("  debt: {} ({})", debt.kind, debt.detail);
        }
        for diagnostic in &root.diagnostics {
            render_attempt_diagnostic_text(diagnostic, "  ");
        }
        if let Some(continuation) = &root.continuation {
            println!("  continuation: {continuation}");
        }
        if let Some(resume_command) = &root.resume_command {
            println!("  resume: {resume_command}");
        }
        println!(
            "  bounds: entries={} bytes={} elapsed_ms={}",
            root.bounds.considered_entries, root.bounds.considered_bytes, root.bounds.elapsed_ms
        );
    }
}

fn render_attempt_diagnostic_text(diagnostic: &AttemptDiagnosticView, indent: &str) {
    println!("{indent}diagnostic: {}", diagnostic.diagnostic_code);
    println!("{indent}failure: {}", diagnostic.failure_code);
    println!("{indent}retry: {}", diagnostic.retry_advice);
    println!("{indent}recover: {}", diagnostic.recovery_command);
}

fn compile_attempt_context(
    pipeline: &std::path::Path,
    identity: &AttemptIdentityArgs,
    execution_id: Option<&str>,
) -> Result<AttemptCommandContext, AttemptCommandError> {
    let pipeline_display = workspace_pipeline_display(pipeline)?;
    let (workspace_root, pipeline_dir, validated_pipeline) =
        if let Some(base_dir) = identity.base_dir.as_deref() {
            let workspace_root = base_dir.canonicalize().map_err(|error| {
                AttemptCommandError::new(format!("cannot resolve workspace root: {error}"))
            })?;
            let validated_pipeline =
                clinker_plan::security::validate_path(pipeline, &workspace_root, false)
                    .map_err(|diagnostic| AttemptCommandError::new(diagnostic.message))?;
            let pipeline_parent = validated_pipeline
                .as_path()
                .parent()
                .unwrap_or(&workspace_root)
                .canonicalize()
                .map_err(|error| {
                    AttemptCommandError::new(format!("cannot resolve pipeline directory: {error}"))
                })?;
            let pipeline_dir = pipeline_parent
                .strip_prefix(&workspace_root)
                .map_err(|_| AttemptCommandError::new("pipeline must remain within the workspace"))?
                .to_path_buf();
            (workspace_root, pipeline_dir, validated_pipeline)
        } else {
            let current_dir = std::env::current_dir().map_err(|error| {
                AttemptCommandError::new(format!("cannot resolve current directory: {error}"))
            })?;
            let pipeline_path = current_dir.join(pipeline).canonicalize().map_err(|error| {
                AttemptCommandError::new(format!("cannot resolve pipeline: {error}"))
            })?;
            let workspace_root = pipeline_path
                .parent()
                .ok_or_else(|| AttemptCommandError::new("pipeline has no parent directory"))?
                .to_path_buf();
            let pipeline_leaf = pipeline_path
                .file_name()
                .ok_or_else(|| AttemptCommandError::new("pipeline has no file name"))?;
            let validated_pipeline = clinker_plan::security::validate_path(
                std::path::Path::new(pipeline_leaf),
                &workspace_root,
                false,
            )
            .map_err(|diagnostic| AttemptCommandError::new(diagnostic.message))?;
            (workspace_root, PathBuf::new(), validated_pipeline)
        };
    let pipeline_path = validated_pipeline.as_path();
    if !pipeline_path.is_file() {
        return Err(AttemptCommandError::new(format!(
            "pipeline does not exist or is not a file: {pipeline_display}"
        )));
    }
    let clinker_toml = clinker_plan::config::ClinkerToml::load_from_workspace(&workspace_root)
        .map_err(|error| AttemptCommandError::new(error.to_string()))?;
    let overlay_resolution = if identity.channel.is_none() && identity.groups.is_empty() {
        None
    } else {
        let catalog =
            clinker_plan::resources::WorkspaceCatalog::load(&workspace_root, &clinker_toml.catalog)
                .map_err(|error| AttemptCommandError::new(error.to_string()))?;
        let pipeline_id =
            catalog_pipeline_id(&workspace_root, &clinker_toml.catalog, pipeline_path)
                .map_err(AttemptCommandError::new)?;
        Some(
            clinker_channel::resolve_target_channel(
                &workspace_root,
                &catalog,
                &clinker_toml.group,
                &pipeline_id,
                identity.channel.as_deref(),
                &identity.groups,
                !identity.no_auto_groups,
            )
            .map_err(|error| {
                AttemptCommandError::new(format!("overlay resolution failed: {error}"))
            })?,
        )
    };
    let empty_patches = indexmap::IndexMap::new();
    let source_patches = overlay_resolution
        .as_ref()
        .and_then(clinker_channel::OverlayResolution::source_patches)
        .unwrap_or(&empty_patches);
    let mut pipeline_config =
        clinker_plan::config::load_config_with_vars_and_patches(pipeline_path, &[], source_patches)
            .map_err(|error| AttemptCommandError::new(error.to_string()))?;
    let template_context = clinker_plan::config::path_template::TemplateContext {
        source_name_default: None,
        source_name_by_node: std::collections::HashMap::new(),
        channel: overlay_resolution
            .as_ref()
            .and_then(|overlay| overlay.channel_id()),
        pipeline_hash: pipeline_config.source_hash,
        timestamp: identity.timestamp.as_deref(),
        execution_id,
        batch_id: identity.batch_id.as_deref(),
        n: None,
        unique_suffix_width: 0,
    };
    clinker_plan::config::path_template::resolve_output_path_templates_in_place(
        &mut pipeline_config,
        &template_context,
    )
    .map_err(|error| AttemptCommandError::new(error.to_string()))?;
    let mut compile_context = clinker_plan::config::CompileContext::with_pipeline_dir(
        workspace_root.clone(),
        pipeline_dir.clone(),
    );
    compile_context.allow_absolute_paths = identity.allow_absolute_paths;
    if let Some(overlay) = &overlay_resolution {
        compile_context.overlay_ops = overlay.op_stream().to_vec();
    }
    let clinker_plan::resources::CompositionDiscovery { fields, identities } =
        clinker_plan::resources::collect_cxl_fields_with_composition_identities(
            &pipeline_config.nodes,
            compile_context.workspace_root(),
            &compile_context.pipeline_dir,
        )
        .map_err(|error| AttemptCommandError::new(error.to_string()))?;
    compile_context.composition_body_identities = identities;
    let direct_imports = clinker_plan::resources::collect_direct_imports(&fields)
        .map_err(|error| AttemptCommandError::new(error.to_string()))?;
    if !direct_imports.is_empty() {
        let catalog = clinker_plan::resources::WorkspaceCatalog::load(
            compile_context.workspace_root(),
            &clinker_toml.catalog,
        )
        .map_err(|error| AttemptCommandError::new(error.to_string()))?;
        let rules_root = catalog
            .select_rules_root(
                identity.rules_path.as_deref(),
                pipeline_config
                    .pipeline
                    .rules_path
                    .as_deref()
                    .map(std::path::Path::new),
            )
            .map_err(|error| AttemptCommandError::new(error.to_string()))?;
        compile_context.cxl_modules = clinker_plan::resources::compile_module_closure(
            &catalog,
            &rules_root,
            &direct_imports,
            clinker_plan::resources::ModuleLimits::default(),
        )
        .map_err(|error| AttemptCommandError::new(error.to_string()))?;
    }
    if let Some(overlay) = &overlay_resolution {
        resolve_overlay_config_before_compile(&pipeline_config, &mut compile_context, overlay)
            .map_err(|error| AttemptCommandError::new(error.to_string()))?;
    }
    let mut compiled_plan = pipeline_config
        .compile(&compile_context)
        .map_err(|diagnostics| {
            let rendered = diagnostics
                .iter()
                .map(|diagnostic| format!("[{}] {}", diagnostic.code, diagnostic.message))
                .collect::<Vec<_>>()
                .join("; ");
            AttemptCommandError::new(format!("pipeline compilation failed: {rendered}"))
        })?;
    if let Some(overlay) = &overlay_resolution {
        let result = overlay.apply_config_and_vars(&mut compiled_plan, &pipeline_config);
        let errors = result
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity == clinker_core_types::Severity::Error)
            .map(|diagnostic| format!("[{}] {}", diagnostic.code, diagnostic.message))
            .collect::<Vec<_>>();
        if !errors.is_empty() {
            return Err(AttemptCommandError::new(format!(
                "overlay application failed: {}",
                errors.join("; ")
            )));
        }
    }

    let pipeline_base = workspace_root.join(&pipeline_dir);
    let receipt_root =
        clinker_plan::security::validate_path(std::path::Path::new("."), &pipeline_base, false)
            .map_err(|diagnostic| AttemptCommandError::new(diagnostic.message))?;
    let historical_receipts = clinker_exec::output::attempt::discover_retained_root_receipts(
        &compiled_plan,
        &receipt_root,
        execution_id,
        observed_unix_ms()?,
    )
    .map_err(attempt_query_error)?;
    let mut source_files_by_name = std::collections::BTreeMap::new();
    for body in pipeline_config.source_bodies() {
        let source = &body.source;
        if !source.transport.is_file() {
            source_files_by_name.insert(source.name.clone(), Vec::new());
            continue;
        }
        let outcome = match clinker_plan::config::discovery::discover(source, &pipeline_base) {
            Ok(outcome) => Some(outcome),
            Err(clinker_plan::config::discovery::DiscoveryError::NoMatch { .. })
                if historical_receipts.iter().any(|receipt| {
                    receipt
                        .historical_sources()
                        .iter()
                        .any(|identity| identity.source_name() == source.name)
                }) =>
            {
                None
            }
            Err(error) => {
                return Err(AttemptCommandError::new(format!(
                    "source '{}' discovery failed while reconstructing attempt roots: {error}",
                    source.name
                )));
            }
        };
        source_files_by_name.insert(
            source.name.clone(),
            outcome
                .as_ref()
                .map_or(
                    &[][..],
                    clinker_plan::config::discovery::DiscoveryOutcome::files,
                )
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>(),
        );
    }
    let mut destination_roots = std::collections::BTreeMap::new();
    for output in compiled_plan.config().output_configs() {
        if output.has_per_record_path_tokens() {
            let upstream_source = upstream_source_for_output(compiled_plan.dag(), &output.name)
                .ok_or_else(|| {
                    AttemptCommandError::new(format!(
                        "output {:?} has per-source path tokens but no reconstructible file source",
                        output.name
                    ))
                })?;
            let files = source_files_by_name.get(&upstream_source).ok_or_else(|| {
                AttemptCommandError::new(format!(
                    "output {:?} references undiscovered source {upstream_source:?}",
                    output.name
                ))
            })?;
            if files.is_empty()
                && !historical_receipts.iter().any(|receipt| {
                    receipt
                        .historical_sources()
                        .iter()
                        .any(|identity| identity.source_name() == upstream_source)
                })
            {
                return Err(AttemptCommandError::new(format!(
                    "output {:?} has per-source path tokens but source {upstream_source:?} has no discovered files",
                    output.name
                )));
            }
            for source_path in files {
                let source_file = source_path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or("source");
                let rendered = output
                    .render_runtime_path(source_file, &source_path.to_string_lossy())
                    .map_err(|error| AttemptCommandError::new(error.to_string()))?;
                insert_attempt_root(
                    &mut destination_roots,
                    &rendered,
                    &pipeline_base,
                    identity.allow_absolute_paths,
                )?;
            }
        } else {
            insert_attempt_root(
                &mut destination_roots,
                &output.path,
                &pipeline_base,
                identity.allow_absolute_paths,
            )?;
        }
    }
    if let Some(dlq) = &compiled_plan.config().error_handling.dlq {
        if let Some(path) = &dlq.path {
            insert_attempt_root(
                &mut destination_roots,
                path,
                &pipeline_base,
                identity.allow_absolute_paths,
            )?;
        }
        for source in dlq.per_source.values() {
            if let Some(path) = &source.path {
                insert_attempt_root(
                    &mut destination_roots,
                    path,
                    &pipeline_base,
                    identity.allow_absolute_paths,
                )?;
            }
        }
    }
    if destination_roots.is_empty() && historical_receipts.is_empty() {
        return Err(AttemptCommandError::new(
            "compiled pipeline has no file destination roots",
        ));
    }

    let policy = clinker_toml.storage.publication;
    let mut resolved_policy = None;
    for root in destination_roots
        .values()
        .filter(|root| root.as_path().is_dir())
    {
        let resolved = policy
            .resolve(root.as_path(), 0, u64::MAX)
            .map_err(|error| AttemptCommandError::new(error.to_string()))?;
        resolved_policy.get_or_insert(resolved);
    }
    if resolved_policy.is_none() {
        let resolved = policy
            .resolve(&pipeline_base, 0, u64::MAX)
            .map_err(|error| AttemptCommandError::new(error.to_string()))?;
        resolved_policy = Some(resolved);
    }
    if let Some(spool) = resolved_policy
        .as_ref()
        .and_then(clinker_plan::config::ResolvedPublicationPolicy::local_spool_dir)
    {
        let root = clinker_plan::security::validate_path(std::path::Path::new("."), spool, false)
            .map_err(|diagnostic| AttemptCommandError::new(diagnostic.message))?;
        destination_roots.insert(root.as_path().to_path_buf(), root);
    }
    for receipt in &historical_receipts {
        for root in reconstruct_historical_receipt_roots(
            &compiled_plan,
            receipt,
            &pipeline_base,
            identity.allow_absolute_paths,
            resolved_policy
                .as_ref()
                .and_then(clinker_plan::config::ResolvedPublicationPolicy::local_spool_dir),
        )? {
            destination_roots.insert(root.as_path().to_path_buf(), root);
        }
    }
    if !historical_receipts.is_empty() {
        destination_roots.insert(receipt_root.as_path().to_path_buf(), receipt_root);
    }

    let existing_roots = destination_roots
        .into_values()
        .filter(|root| root.as_path().is_dir())
        .collect::<Vec<_>>();
    let query = if existing_roots.is_empty() {
        None
    } else {
        Some(
            clinker_exec::output::attempt::AttemptQuery::new(
                &compiled_plan,
                resolved_policy
                    .as_ref()
                    .expect("publication policy was resolved above"),
                existing_roots,
            )
            .map_err(attempt_query_error)?,
        )
    };
    let root_ids = query
        .as_ref()
        .map(|query| {
            query
                .owned_root_ids()
                .into_iter()
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    Ok(AttemptCommandContext {
        query,
        root_ids,
        workspace_root,
        pipeline: pipeline_display,
        identity_argv: attempt_identity_argv(identity),
    })
}

fn attempt_identity_argv(identity: &AttemptIdentityArgs) -> Vec<String> {
    let mut argv = Vec::new();
    if let Some(base_dir) = &identity.base_dir {
        argv.extend([
            "--base-dir".to_owned(),
            base_dir.to_string_lossy().into_owned(),
        ]);
    }
    if identity.allow_absolute_paths {
        argv.push("--allow-absolute-paths".to_owned());
    }
    if let Some(rules_path) = &identity.rules_path {
        argv.extend([
            "--rules-path".to_owned(),
            rules_path.to_string_lossy().into_owned(),
        ]);
    }
    if let Some(channel) = &identity.channel {
        argv.extend(["--channel".to_owned(), channel.clone()]);
    }
    for group in &identity.groups {
        argv.extend(["--group".to_owned(), group.clone()]);
    }
    if identity.no_auto_groups {
        argv.push("--no-auto-groups".to_owned());
    }
    if let Some(execution_id) = &identity.path_execution_id {
        argv.extend(["--path-execution-id".to_owned(), execution_id.clone()]);
    }
    if let Some(batch_id) = &identity.batch_id {
        argv.extend(["--batch-id".to_owned(), batch_id.clone()]);
    }
    if let Some(timestamp) = &identity.timestamp {
        argv.extend(["--timestamp".to_owned(), timestamp.clone()]);
    }
    argv
}

fn insert_attempt_root(
    roots: &mut std::collections::BTreeMap<PathBuf, clinker_plan::security::ValidatedPath>,
    authored_path: &str,
    pipeline_base: &std::path::Path,
    allow_absolute_paths: bool,
) -> Result<(), AttemptCommandError> {
    let path = std::path::Path::new(authored_path);
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let parent_text = parent.to_string_lossy();
    if parent_text.contains(['{', '}']) || authored_path.contains("{source_path}") {
        return Err(AttemptCommandError::new(format!(
            "attempt operations require a static destination directory; {authored_path:?} can resolve to more than one directory"
        )));
    }
    let validated = clinker_plan::security::validate_path(
        parent,
        pipeline_base,
        allow_absolute_paths && parent.is_absolute(),
    )
    .map_err(|diagnostic| AttemptCommandError::new(diagnostic.message))?;
    roots.insert(validated.as_path().to_path_buf(), validated);
    Ok(())
}

fn reconstruct_historical_receipt_roots(
    compiled_plan: &clinker_plan::plan::CompiledPlan,
    receipt: &clinker_exec::output::attempt::RetainedRootReceipt,
    pipeline_base: &std::path::Path,
    allow_absolute_paths: bool,
    local_spool_dir: Option<&std::path::Path>,
) -> Result<Vec<clinker_plan::security::ValidatedPath>, AttemptCommandError> {
    let mut roots = std::collections::BTreeMap::new();
    for output in compiled_plan.config().output_configs() {
        if output.has_per_record_path_tokens() {
            let upstream_source = upstream_source_for_output(compiled_plan.dag(), &output.name)
                .ok_or_else(|| {
                    AttemptCommandError::new(format!(
                        "output {:?} has historical path tokens but no reconstructible file source",
                        output.name
                    ))
                })?;
            let sources = receipt
                .historical_sources()
                .iter()
                .filter(|identity| identity.source_name() == upstream_source)
                .collect::<Vec<_>>();
            if sources.is_empty() {
                return Err(AttemptCommandError::new(format!(
                    "retained execution {} has no historical source identity for output {:?}",
                    receipt.execution_id(),
                    output.name
                )));
            }
            for identity in sources {
                let source_path = std::path::Path::new(identity.source_path());
                let source_file = source_path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or("source");
                let rendered = output
                    .render_runtime_path(source_file, identity.source_path())
                    .map_err(|error| AttemptCommandError::new(error.to_string()))?;
                insert_attempt_root(&mut roots, &rendered, pipeline_base, allow_absolute_paths)?;
            }
        } else {
            insert_attempt_root(
                &mut roots,
                &output.path,
                pipeline_base,
                allow_absolute_paths,
            )?;
        }
    }
    if let Some(dlq) = &compiled_plan.config().error_handling.dlq {
        if let Some(path) = &dlq.path {
            insert_attempt_root(&mut roots, path, pipeline_base, allow_absolute_paths)?;
        }
        for source in dlq.per_source.values() {
            if let Some(path) = &source.path {
                insert_attempt_root(&mut roots, path, pipeline_base, allow_absolute_paths)?;
            }
        }
    }
    if let Some(spool) = local_spool_dir {
        let root = clinker_plan::security::validate_path(std::path::Path::new("."), spool, false)
            .map_err(|diagnostic| AttemptCommandError::new(diagnostic.message))?;
        roots.insert(root.as_path().to_path_buf(), root);
    }
    let roots = roots.into_values().collect::<Vec<_>>();
    receipt
        .authenticate_roots(&roots)
        .map_err(attempt_query_error)?;
    Ok(roots)
}

fn workspace_pipeline_display(pipeline: &std::path::Path) -> Result<String, AttemptCommandError> {
    if pipeline.is_absolute()
        || pipeline.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(AttemptCommandError::new(
            "pipeline must be a traversal-free workspace-relative .yaml or .yml path",
        ));
    }
    let display = pipeline.to_string_lossy();
    if display.is_empty()
        || display.contains(['\\', '\0', '\n', '\r'])
        || !(display.ends_with(".yaml") || display.ends_with(".yml"))
    {
        return Err(AttemptCommandError::new(
            "pipeline must be a traversal-free workspace-relative .yaml or .yml path",
        ));
    }
    Ok(display.into_owned())
}

fn validate_execution_selector(execution_id: &str) -> Result<(), AttemptCommandError> {
    let valid = execution_id.len() == 36
        && execution_id
            .bytes()
            .enumerate()
            .all(|(index, byte)| match index {
                8 | 13 | 18 | 23 => byte == b'-',
                _ => byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase(),
            });
    if valid {
        Ok(())
    } else {
        Err(AttemptCommandError::new(
            "--execution-id must be a canonical lowercase UUID",
        ))
    }
}

fn observed_unix_ms() -> Result<u64, AttemptCommandError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| AttemptCommandError::new("system clock is earlier than the Unix epoch"))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| AttemptCommandError::new("system clock exceeds the supported range"))
}

fn decode_attempt_continuation(
    continuation: Option<&str>,
) -> Result<Option<clinker_exec::output::attempt::AttemptContinuation>, AttemptCommandError> {
    continuation
        .map(|value| {
            clinker_exec::output::attempt::AttemptContinuation::from_bytes(value.as_bytes())
                .map_err(|error| {
                    AttemptCommandError::new(format!("invalid --continuation: {error}"))
                })
        })
        .transpose()
}

fn encode_attempt_continuation(
    continuation: Option<&clinker_exec::output::attempt::AttemptContinuation>,
) -> Result<Option<String>, AttemptCommandError> {
    continuation
        .map(|continuation| {
            let bytes = continuation.to_bytes().map_err(attempt_query_error)?;
            String::from_utf8(bytes)
                .map_err(|_| AttemptCommandError::new("attempt continuation is not valid UTF-8"))
        })
        .transpose()
}

fn purge_resume_command(args: &AttemptsPurgeArgs, pipeline: &str, continuation: &str) -> String {
    render_attempt_command(&purge_resume_argv(args, pipeline, continuation))
}

fn purge_resume_argv(args: &AttemptsPurgeArgs, pipeline: &str, continuation: &str) -> Vec<String> {
    let mut argv = vec![
        "clinker".to_owned(),
        "attempts".to_owned(),
        "purge".to_owned(),
        pipeline.to_owned(),
    ];
    match args.execution_id.as_deref() {
        Some(execution_id) => {
            argv.push("--execution-id".to_owned());
            argv.push(execution_id.to_owned());
        }
        None => argv.push("--expired".to_owned()),
    }
    if args.execute {
        argv.push("--execute".to_owned());
    }
    argv.extend(attempt_identity_argv(&args.identity));
    argv.push("--continuation".to_owned());
    argv.push(continuation.to_owned());
    argv
}

fn render_attempt_command(argv: &[String]) -> String {
    argv.iter()
        .map(|argument| quote_attempt_argument(argument))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(not(windows))]
fn quote_attempt_argument(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
    {
        return value.to_owned();
    }
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for character in value.chars() {
        if character == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(character);
        }
    }
    quoted.push('\'');
    quoted
}

#[cfg(windows)]
fn quote_attempt_argument(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
    {
        return value.to_owned();
    }
    quote_windows_argument(value)
}

#[cfg_attr(not(windows), allow(dead_code))]
fn quote_windows_argument(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    let mut backslashes = 0_usize;
    for character in value.chars() {
        match character {
            '\\' => backslashes += 1,
            '"' => {
                for _ in 0..backslashes {
                    quoted.push('\\');
                    quoted.push('\\');
                }
                quoted.push('\\');
                quoted.push('"');
                backslashes = 0;
            }
            _ => {
                for _ in 0..backslashes {
                    quoted.push('\\');
                }
                backslashes = 0;
                quoted.push(character);
            }
        }
    }
    for _ in 0..backslashes {
        quoted.push('\\');
        quoted.push('\\');
    }
    quoted.push('"');
    quoted
}

fn attempt_query_error(error: clinker_exec::output::attempt::AttemptError) -> AttemptCommandError {
    AttemptCommandError::new(error.to_string())
}

fn root_has_debt(root: &AttemptRootView) -> bool {
    !root.cleanup_debt.is_empty()
        || root
            .attempts
            .iter()
            .any(|attempt| !attempt.cleanup_debt.is_empty())
        || root.continuation.is_some()
}

fn sanitize_attempt_path(path: &std::path::Path, workspace_root: &std::path::Path) -> String {
    let Ok(relative) = path.strip_prefix(workspace_root) else {
        return "<redacted-outside-workspace>".to_owned();
    };
    let mut sanitized = PathBuf::new();
    for component in relative.components() {
        let text = component.as_os_str().to_string_lossy();
        let lowercase = text.to_ascii_lowercase();
        if [
            "secret",
            "token",
            "password",
            "credential",
            "apikey",
            "api_key",
        ]
        .iter()
        .any(|marker| lowercase.contains(marker))
        {
            sanitized.push("<redacted>");
        } else {
            sanitized.push(component.as_os_str());
        }
    }
    if sanitized.as_os_str().is_empty() {
        ".".to_owned()
    } else {
        sanitized.to_string_lossy().into_owned()
    }
}

fn attempt_state_name(state: clinker_exec::output::attempt::AttemptState) -> &'static str {
    use clinker_exec::output::attempt::AttemptState;
    match state {
        AttemptState::Staging => "staging",
        AttemptState::Ready => "ready",
        AttemptState::Publishing => "publishing",
        AttemptState::Complete => "complete",
        AttemptState::Incomplete => "incomplete",
        AttemptState::Abandoned => "abandoned",
    }
}

fn cleanup_disposition_name(
    disposition: clinker_exec::output::attempt::CleanupDisposition,
) -> &'static str {
    use clinker_exec::output::attempt::CleanupDisposition;
    match disposition {
        CleanupDisposition::Removed => "removed",
        CleanupDisposition::AlreadyAbsent => "already_absent",
        CleanupDisposition::Kept => "kept",
    }
}

fn purge_disposition_name(
    disposition: clinker_exec::output::attempt::PurgeDisposition,
) -> &'static str {
    use clinker_exec::output::attempt::PurgeDisposition;
    match disposition {
        PurgeDisposition::Removed => "removed",
        PurgeDisposition::AlreadyAbsent => "already_absent",
        PurgeDisposition::Kept => "kept",
        PurgeDisposition::Partial => "partial",
    }
}

fn cleanup_debt_kind_name(kind: clinker_exec::output::attempt::CleanupDebtKind) -> &'static str {
    use clinker_exec::output::attempt::CleanupDebtKind;
    match kind {
        CleanupDebtKind::EntryBudget => "entry_budget",
        CleanupDebtKind::ByteBudget => "byte_budget",
        CleanupDebtKind::TimeBudget => "time_budget",
        CleanupDebtKind::MonotonicClock => "monotonic_clock",
        CleanupDebtKind::LiveAttempt => "live_attempt",
        CleanupDebtKind::InvalidOwnership => "invalid_ownership",
        CleanupDebtKind::InvalidManifest => "invalid_manifest",
        CleanupDebtKind::UnknownChild => "unknown_child",
        CleanupDebtKind::UnsafeEntry => "unsafe_entry",
        CleanupDebtKind::ClockAmbiguous => "clock_ambiguous",
        CleanupDebtKind::Operational => "operational",
        CleanupDebtKind::Interrupted => "interrupted",
    }
}

fn failure_code_for_debt(kind: clinker_exec::output::attempt::CleanupDebtKind) -> &'static str {
    use clinker_exec::output::attempt::CleanupDebtKind;
    match kind {
        CleanupDebtKind::InvalidManifest => "attempt.retention.manifest_invalid",
        CleanupDebtKind::LiveAttempt => "attempt.retention.live",
        CleanupDebtKind::MonotonicClock | CleanupDebtKind::ClockAmbiguous => {
            "attempt.retention.clock_ambiguous"
        }
        CleanupDebtKind::EntryBudget
        | CleanupDebtKind::ByteBudget
        | CleanupDebtKind::TimeBudget
        | CleanupDebtKind::Interrupted => "attempt.retention.budget_exhausted",
        CleanupDebtKind::Operational => "attempt.retention.cleanup_failed",
        CleanupDebtKind::InvalidOwnership
        | CleanupDebtKind::UnknownChild
        | CleanupDebtKind::UnsafeEntry => "attempt.retention.ownership_refused",
    }
}

/// `clinker config` — inspect / canonicalize a pipeline config.
///
/// The only mode today is `--resolved`, which expands the multi-value shorthand
/// in place. The config is loaded and validated first so a malformed file fails
/// with a real config error rather than emitting a half-canonicalized document;
/// the raw text is then rewritten surgically (only the shorthand sequences
/// change) and printed to stdout.
fn run_config(args: &ConfigArgs) -> Result<u8, Box<dyn std::error::Error>> {
    if !args.resolved {
        return Err(
            "nothing to do: pass --resolved to print the canonical, fully-expanded config".into(),
        );
    }

    // Read the source once and reuse it for both validation and the rewrite.
    // A second read would reopen the file after validation, leaving a TOCTOU
    // window in which the resolved output could reflect different bytes than the
    // ones just validated.
    let raw = std::fs::read_to_string(&args.config)
        .map_err(|e| format!("{}: {e}", args.config.display()))?;

    // Validate before rewriting: a parse or schema error surfaces here rather
    // than producing a document that no longer matches a loadable plan.
    clinker_plan::config::load_config_from_str(&raw)
        .map_err(|e| format!("{}: {e}", args.config.display()))?;

    let resolved = clinker_plan::config::expand_multi_value_shorthand(&raw)
        .map_err(|e| format!("{}: {e}", args.config.display()))?;
    print!("{resolved}");
    Ok(0)
}

fn run_explain(args: &ExplainArgs) -> Result<(), Box<dyn std::error::Error>> {
    // Mode 1: --code — look up error/warning code documentation.
    if let Some(ref code) = args.code {
        match clinker_plan::plan::explain_provenance::explain_code(code) {
            Some(doc) => {
                print!("{doc}");
                return Ok(());
            }
            None => {
                // Derived from the same table `explain_code` answers from --
                // a retyped range goes stale the first time a code is added,
                // and then contradicts the `See: clinker explain --code <CODE>`
                // hint the run path now prints.
                let valid = clinker_plan::plan::explain_provenance::explain_codes().join(", ");
                return Err(
                    format!("unknown diagnostic code '{code}'. Valid codes: {valid}").into(),
                );
            }
        }
    }

    // Mode 2: --field — field provenance chain.
    let config_path = args.config.as_ref().ok_or(
        "a pipeline config path is required when using --field (usage: clinker explain pipeline.yaml --field node.param)",
    )?;

    let field = args.field.as_ref().ok_or(
        "either --field or --code is required (usage: clinker explain pipeline.yaml --field node.param)",
    )?;

    let yaml = std::fs::read_to_string(config_path)?;
    let interpolated = clinker_plan::config::interpolate_env_vars(&yaml, &[])
        .map_err(|e| format!("environment variable interpolation failed: {e}"))?;
    let mut pipeline_config: clinker_plan::config::PipelineConfig =
        clinker_plan::yaml::from_str(&interpolated)
            .map_err(|e| format!("YAML parse error: {e}"))?;

    // Resolve workspace root and pipeline_dir so composition `use:` paths
    // resolve correctly. The workspace root is the base_dir (default: CWD),
    // and pipeline_dir is the config file's parent relative to workspace_root.
    let workspace_root = args.base_dir.canonicalize()?;
    let config_parent = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."))
        .canonicalize()?;
    let pipeline_dir = config_parent
        .strip_prefix(&workspace_root)
        .unwrap_or_else(|_| std::path::Path::new(""))
        .to_path_buf();

    // Resolve the channel/group overlay stack. A `--channel <id>` selects a
    // tenant by computed path (deriving matching groups from its labels);
    // `--group <name>` force-includes a group. The op stream and `config`/`vars`
    // clobber apply before provenance is computed, mirroring `run`.
    let clinker_toml = clinker_plan::config::ClinkerToml::load_from_workspace(&workspace_root)
        .map_err(|e| format!("clinker.toml: {e}"))?;
    let overlay_resolution = if args.channel.is_none() && args.groups.is_empty() {
        None
    } else {
        let catalog = clinker_plan::resources::WorkspaceCatalog::load(
            &workspace_root,
            &clinker_toml.catalog,
        )?;
        let pipeline_id = catalog_pipeline_id(&workspace_root, &clinker_toml.catalog, config_path)?;
        Some(
            clinker_channel::resolve_target_channel(
                &workspace_root,
                &catalog,
                &clinker_toml.group,
                &pipeline_id,
                args.channel.as_deref(),
                &args.groups,
                !args.no_auto_groups,
            )
            .map_err(|e| format!("overlay resolution failed: {e}"))?,
        )
    };

    // Apply the resolved channel's per-source config patches before compile, so
    // provenance is computed against the same patched plan a `run` would
    // execute — no explain path compiles an unpatched config.
    if let Some(patches) = overlay_resolution.as_ref().and_then(|r| r.source_patches()) {
        clinker_plan::config::apply_source_patches(&mut pipeline_config, patches)
            .map_err(|e| format!("{e}"))?;
    }

    let mut compile_ctx =
        clinker_plan::config::CompileContext::with_pipeline_dir(&workspace_root, pipeline_dir);
    if let Some(res) = &overlay_resolution {
        compile_ctx.overlay_ops = res.op_stream().to_vec();
        resolve_overlay_config_before_compile(&pipeline_config, &mut compile_ctx, res)
            .map_err(|error| format!("overlay validation failed: {error}"))?;
    }

    // Compile failures render through the same path `clinker run` uses, so a
    // user who hits a gate here gets the code, help and source line rather
    // than a bare message with no way to reach `clinker explain --code`.
    let anchors_trusted = plan_line_anchors_trusted(
        &pipeline_config,
        overlay_contributed(overlay_resolution.as_ref()),
    );
    let mut compiled_plan = pipeline_config.compile(&compile_ctx).map_err(|diags| {
        render_pipeline_error(&plan_diagnostics(diags, anchors_trusted), config_path);
        AlreadyReported
    })?;

    if let Some(res) = &overlay_resolution {
        let overlay = res.apply_config_and_vars(&mut compiled_plan, &pipeline_config);
        if let Err(e) = abort_on_overlay_errors(&overlay) {
            render_pipeline_error(&e, config_path);
            return Err(AlreadyReported.into());
        }
    }

    let output =
        clinker_plan::plan::explain_provenance::explain_field_provenance(&compiled_plan, field)
            .map_err(|e| format!("{e}"))?;

    print!("{output}");
    Ok(())
}

/// One-line summary of an applied overlay resolution for run/explain output.
fn overlay_summary(res: &clinker_channel::OverlayResolution) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(id) = res.channel_id() {
        parts.push(format!("channel {id}"));
    }
    if res.applied_groups().is_empty() {
        parts.push("no groups".to_string());
    } else {
        let groups: Vec<String> = res
            .applied_groups()
            .iter()
            .map(|g| format!("{} ({}, priority {})", g.name, g.source.label(), g.priority))
            .collect();
        parts.push(format!("groups: {}", groups.join(", ")));
    }
    parts.join("; ")
}

/// Human-readable label for an op-stream overlay layer.
fn op_layer_label(layer: clinker_plan::overlay_ops::OverlayLayer) -> String {
    use clinker_plan::overlay_ops::OverlayLayer;
    match layer {
        OverlayLayer::PipelineDefault => "pipeline-default".to_string(),
        OverlayLayer::Group { priority } => format!("group (priority {priority})"),
        OverlayLayer::ChannelWide => "channel-wide".to_string(),
        OverlayLayer::ChannelPerTarget => "channel-per-target".to_string(),
    }
}

/// Format a JSON value for the provenance table (strip quotes from strings).
fn format_overlay_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => format!("\"{s}\""),
        other => other.to_string(),
    }
}

/// Compile the effective (post-overlay) plan for a target under a resolution.
///
/// Applies the resolved per-source `sources:` patches and the structural op
/// stream pre-compile (via `overlay_ops`) and the `config`/`vars` clobber
/// post-compile, returning the config, the compiled plan, and the overlay
/// result (whose diagnostics carry any `E113`/`E109`). A compile failure (e.g.
/// a dangling splice anchor) is a hard `Err`.
/// Why [`compile_effective_plan`] could not produce an effective plan.
///
/// Compile-gate diagnostics are carried whole rather than flattened to a
/// string: `channels resolve` renders one report per diagnostic, with the same
/// code, help, and explain pointer the identical failure gets under `run`,
/// while `channels lint` folds them into its per-target failure table. Both
/// need the parts, and a joined string has already thrown them away.
enum EffectivePlanError {
    /// Diagnostics from `config.compile()`.
    Diagnostics(Vec<clinker_core_types::Diagnostic>),
    /// A failure before compile — unreadable base file, `${VAR}` with nothing
    /// behind it, YAML syntax, or a source patch that would not apply. These
    /// carry no diagnostic code, so there is nothing to preserve.
    Setup(String),
}

impl EffectivePlanError {
    /// One line per underlying failure, for a caller assembling a summary
    /// table rather than rendering a report.
    fn lines(&self) -> Vec<String> {
        match self {
            Self::Diagnostics(diags) => diags
                .iter()
                .map(|d| format!("[{}] {}", d.code, d.message))
                .collect(),
            Self::Setup(msg) => msg.lines().map(str::to_owned).collect(),
        }
    }
}

fn compile_effective_plan(
    base_path: &std::path::Path,
    workspace_root: &std::path::Path,
    res: &clinker_channel::OverlayResolution,
) -> Result<
    (
        clinker_plan::config::PipelineConfig,
        clinker_plan::plan::CompiledPlan,
        clinker_channel::ChannelOverlayResult,
    ),
    EffectivePlanError,
> {
    use EffectivePlanError as E;
    let yaml = std::fs::read_to_string(base_path)
        .map_err(|e| E::Setup(format!("cannot read {}: {e}", base_path.display())))?;
    let interpolated = clinker_plan::config::interpolate_env_vars(&yaml, &[])
        .map_err(|e| E::Setup(format!("environment variable interpolation failed: {e}")))?;
    let mut config: clinker_plan::config::PipelineConfig =
        clinker_plan::yaml::from_str(&interpolated)
            .map_err(|e| E::Setup(format!("YAML parse error: {e}")))?;

    // Per-source patches shape the parsed config before compile, so the
    // effective DAG this reports reflects the same schema / multi-value /
    // options changes a `run --channel` would execute.
    if let Some(patches) = res.source_patches() {
        clinker_plan::config::apply_source_patches(&mut config, patches)
            .map_err(|e| E::Setup(e.to_string()))?;
    }

    let base_parent = base_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| std::path::PathBuf::from("."));
    let pipeline_dir = base_parent
        .strip_prefix(workspace_root)
        .unwrap_or_else(|_| std::path::Path::new(""))
        .to_path_buf();
    let mut ctx =
        clinker_plan::config::CompileContext::with_pipeline_dir(workspace_root, pipeline_dir);
    ctx.overlay_ops = res.op_stream().to_vec();

    let validation_plan = config.compile(&ctx).map_err(E::Diagnostics)?;
    let resolved_config = res
        .resolve_config(&validation_plan)
        .map_err(E::Diagnostics)?;
    ctx.config_overrides = resolved_config.into_compile_overrides();

    let mut plan = config.compile(&ctx).map_err(E::Diagnostics)?;
    let overlay = res.apply_config_and_vars(&mut plan, &config);
    Ok((config, plan, overlay))
}

/// Render the effective post-overlay plan with per-node/op provenance — the
/// `channels resolve` report. Deterministic (no file sizes / timing), so it
/// snapshots cleanly.
fn render_resolved(
    plan: &clinker_plan::plan::CompiledPlan,
    overlay: &clinker_channel::ChannelOverlayResult,
    res: &clinker_channel::OverlayResolution,
    target_name: &str,
) -> String {
    use clinker_core_types::Severity;
    use clinker_plan::config::composition::LayerKind;

    let mut out = String::new();
    out.push_str(&format!("Effective plan for `{target_name}`\n"));
    match res.channel_id() {
        Some(id) => out.push_str(&format!("  channel: {id}\n")),
        None => out.push_str("  channel: <none>\n"),
    }
    if res.applied_groups().is_empty() {
        out.push_str("  groups:  <none>\n");
    } else {
        out.push_str("  groups:\n");
        for g in res.applied_groups() {
            out.push_str(&format!(
                "    - {} (priority {}, {})\n",
                g.name,
                g.priority,
                g.source.label()
            ));
        }
    }
    out.push('\n');

    out.push_str("Injected nodes:\n");
    if res.injected_nodes().is_empty() {
        out.push_str("  <none>\n");
    } else {
        for inj in res.injected_nodes() {
            out.push_str(&format!(
                "  {} <- {} [{}]\n",
                inj.node,
                inj.source,
                op_layer_label(inj.layer)
            ));
        }
    }
    out.push('\n');

    out.push_str("Config provenance (overlay-affected):\n");
    let mut rows: Vec<String> = Vec::new();
    for (_key, address, resolved) in plan.provenance().iter() {
        let Some(win) = resolved.winning_layer() else {
            continue;
        };
        if win.kind == LayerKind::PipelineDefault {
            continue;
        }
        let base = resolved
            .layer_value(LayerKind::PipelineDefault)
            .map(format_overlay_value)
            .unwrap_or_else(|| "<none>".to_string());
        // Surface the per-value lock: a `fixed:` overlay value holds against
        // every higher-precedence layer, so the winning layer here may be a
        // lower one than plain precedence would pick.
        let lock = if win.fixed { " (fixed)" } else { "" };
        let display_field = if address.call_path().is_empty() {
            format!("{}.{}", address.node_name(), address.field().name())
        } else {
            address.render()
        };
        rows.push(format!(
            "  {} = {}  [{}]{lock}  (base: {})",
            display_field,
            format_overlay_value(&resolved.value),
            win.kind,
            base
        ));
    }
    rows.sort();
    if rows.is_empty() {
        out.push_str("  <none>\n");
    } else {
        for r in rows {
            out.push_str(&r);
            out.push('\n');
        }
    }

    let diags: Vec<&clinker_core_types::Diagnostic> = overlay
        .diagnostics
        .iter()
        .filter(|d| matches!(d.severity, Severity::Error | Severity::Warning))
        .collect();
    if !diags.is_empty() {
        out.push('\n');
        out.push_str("Diagnostics:\n");
        for d in diags {
            let label = match d.severity {
                Severity::Error => "error",
                Severity::Warning => "warning",
                Severity::Note => "note",
            };
            out.push_str(&format!("  {label}: [{}] {}\n", d.code, d.message));
        }
    }

    out
}

/// `clinker channels resolve <target>` — render the effective post-overlay plan
/// for one target with per-value provenance.
fn run_channels_resolve(args: &ResolveArgs) -> Result<u8, Box<dyn std::error::Error>> {
    let workspace_root = args.base_dir.canonicalize()?;
    let clinker_toml = clinker_plan::config::ClinkerToml::load_from_workspace(&workspace_root)
        .map_err(|e| format!("clinker.toml: {e}"))?;
    let catalog =
        clinker_plan::resources::WorkspaceCatalog::load(&workspace_root, &clinker_toml.catalog)?;
    let pipeline_id = catalog_pipeline_id(&workspace_root, &clinker_toml.catalog, &args.target)?;

    let res = clinker_channel::resolve_target_channel(
        &workspace_root,
        &catalog,
        &clinker_toml.group,
        &pipeline_id,
        args.channel.as_deref(),
        &args.groups,
        !args.no_auto_groups,
    )
    .map_err(|e| format!("overlay resolution failed: {e}"))?;

    // A compile failure renders here rather than propagating as a message, so
    // the identical gate reads the same under `channels resolve` as under
    // `run`: code in the header, help below it, explain pointer attached.
    let (config, plan, overlay) = match compile_effective_plan(&args.target, &workspace_root, &res)
    {
        Ok(triple) => triple,
        Err(EffectivePlanError::Diagnostics(diags)) => {
            // Unanchored: an overlay op numbers lines in the overlay file, and
            // this path always has an overlay applied.
            render_pipeline_error(
                &PipelineError::plan_diagnostics_unanchored(diags),
                &args.target,
            );
            return Err(Box::new(AlreadyReported));
        }
        Err(e @ EffectivePlanError::Setup(_)) => return Err(e.lines().join("\n").into()),
    };

    // Overlay report (deterministic) followed by the effective DAG for context.
    print!("{}", render_resolved(&plan, &overlay, &res, &pipeline_id));
    println!("\nEffective DAG:");
    print!("{}", plan.dag().explain_text(&config));

    // An overlay that raised an error diagnostic (e.g. an unknown config key)
    // resolves to a non-zero exit so `resolve` doubles as a targeted check.
    use clinker_core_types::Severity;
    let has_error = overlay
        .diagnostics
        .iter()
        .any(|d| matches!(d.severity, Severity::Error));
    Ok(if has_error { 1 } else { 0 })
}

/// `clinker channels lint` — compile every (target × overlay) combination in the
/// workspace and report failures. This is the full-tree scan (kept off the run
/// path, which resolves by computed lookup).
fn run_channels_lint(args: &LintArgs) -> Result<u8, Box<dyn std::error::Error>> {
    let workspace_root = args.base_dir.canonicalize()?;
    let clinker_toml = clinker_plan::config::ClinkerToml::load_from_workspace(&workspace_root)
        .map_err(|e| format!("clinker.toml: {e}"))?;

    let catalog =
        clinker_plan::resources::WorkspaceCatalog::load(&workspace_root, &clinker_toml.catalog)?;

    let mut checked = 0usize;
    let mut failures: Vec<(String, String, Vec<String>)> = Vec::new();

    for channel_id in clinker_toml.catalog.channels.keys() {
        let logical_channel = clinker_plan::resources::LogicalResourceId::parse(channel_id)?;
        let channel_dir = catalog.resolve(
            clinker_plan::resources::CatalogResourceKind::Channel,
            &logical_channel,
        )?;
        let manifest = match clinker_channel::ChannelManifest::load(
            &channel_dir.join(clinker_channel::CHANNEL_MANIFEST_FILE),
        ) {
            Ok(manifest) => manifest,
            Err(error) => {
                failures.push((
                    channel_id.clone(),
                    "channel.cfg.yaml".to_string(),
                    vec![format!("manifest error: {error}")],
                ));
                continue;
            }
        };
        for target in &manifest.channel.targets {
            let pipeline_id = target.value.clone();
            let logical_pipeline =
                match clinker_plan::resources::LogicalResourceId::parse(&pipeline_id) {
                    Ok(id) => id,
                    Err(error) => {
                        failures.push((channel_id.clone(), pipeline_id, vec![error.to_string()]));
                        continue;
                    }
                };
            let base_path = match catalog.resolve(
                clinker_plan::resources::CatalogResourceKind::Pipeline,
                &logical_pipeline,
            ) {
                Ok(path) => path.to_path_buf(),
                Err(error) => {
                    failures.push((channel_id.clone(), pipeline_id, vec![error.to_string()]));
                    continue;
                }
            };
            checked += 1;
            let res = match clinker_channel::resolve_target_channel(
                &workspace_root,
                &catalog,
                &clinker_toml.group,
                &pipeline_id,
                Some(channel_id),
                &[],
                true,
            ) {
                Ok(r) => r,
                Err(e) => {
                    failures.push((
                        channel_id.clone(),
                        pipeline_id.clone(),
                        vec![format!("resolution failed: {e}")],
                    ));
                    continue;
                }
            };

            match compile_effective_plan(&base_path, &workspace_root, &res) {
                Ok((_, _, overlay)) => {
                    use clinker_core_types::Severity;
                    let errs: Vec<String> = overlay
                        .diagnostics
                        .iter()
                        .filter(|d| matches!(d.severity, Severity::Error))
                        .map(|d| format!("[{}] {}", d.code, d.message))
                        .collect();
                    if !errs.is_empty() {
                        failures.push((channel_id.clone(), pipeline_id.clone(), errs));
                    }
                }
                Err(e) => {
                    failures.push((channel_id.clone(), pipeline_id.clone(), e.lines()));
                }
            }
        }
    }

    if failures.is_empty() {
        println!(
            "channels lint: OK — {checked} (target × overlay) combination(s) across {} channel(s) compiled clean",
            clinker_toml.catalog.channels.len()
        );
        Ok(0)
    } else {
        for (channel_id, target, msgs) in &failures {
            eprintln!("FAIL  channel `{channel_id}`  target `{target}`");
            for m in msgs {
                eprintln!("        {m}");
            }
        }
        eprintln!(
            "channels lint: {} failure(s) of {checked} combination(s)",
            failures.len()
        );
        Ok(1)
    }
}

/// Find the logical pipeline identity for a CLI path. Runtime overlay
/// selection never derives identity from a filename or current directory.
fn catalog_pipeline_id(
    workspace_root: &std::path::Path,
    catalog: &clinker_plan::resources::CatalogConfig,
    pipeline_path: &std::path::Path,
) -> Result<String, String> {
    let selected = pipeline_path
        .canonicalize()
        .map_err(|error| format!("cannot open selected pipeline: {error}"))?;
    for (id, configured) in &catalog.pipelines {
        let candidate = if configured.is_absolute() {
            configured.clone()
        } else {
            workspace_root.join(configured)
        };
        if candidate.canonicalize().ok().as_deref() == Some(selected.as_path()) {
            return Ok(id.clone());
        }
    }
    Err(
        "selected pipeline is not cataloged; add it to `[catalog.pipelines]` in clinker.toml before using channels or groups"
            .to_string(),
    )
}

/// `clinker channels group members <group>` — list the channels whose labels
/// satisfy a group's selector, evaluated through the same group-derivation
/// machinery the overlay resolver uses.
fn run_channels_group_members(args: &GroupMembersArgs) -> Result<u8, Box<dyn std::error::Error>> {
    let workspace_root = args.base_dir.canonicalize()?;
    let clinker_toml = clinker_plan::config::ClinkerToml::load_from_workspace(&workspace_root)
        .map_err(|e| format!("clinker.toml: {e}"))?;

    let groups = clinker_channel::scan_groups(&clinker_toml.group, &workspace_root)
        .map_err(diag_message("group scan failed"))?;
    let group = groups
        .iter()
        .find(|g| g.name == args.group)
        .ok_or_else(|| {
            let known: Vec<&str> = groups.iter().map(|g| g.name.as_str()).collect();
            format!(
                "no group named `{}` under the group root (known: {})",
                args.group,
                if known.is_empty() {
                    "<none>".to_string()
                } else {
                    known.join(", ")
                }
            )
        })?;

    // An explicit-only group (no `match:`) is never derived — it applies only by
    // name. Report that rather than pretend an empty membership is meaningful.
    let Some(selector) = group.selector.as_deref() else {
        println!(
            "group `{}` has no selector (explicit-only); it has no derived members",
            args.group
        );
        return Ok(0);
    };

    let channels = clinker_channel::scan_channels(&clinker_toml.channel, &workspace_root)
        .map_err(diag_message("channel scan failed"))?;

    let mut matched: Vec<String> = Vec::new();
    let mut errors: Vec<(String, String)> = Vec::new();
    for channel in &channels {
        let labels = channel
            .manifest
            .as_ref()
            .map(|m| m.labels.clone())
            .unwrap_or_default();
        // Route through CH-8 derivation so a selector error (e.g. a channel
        // missing a referenced label) surfaces transparently, never as a silent
        // non-match.
        let derivation = clinker_channel::derive_groups(std::slice::from_ref(group), &labels);
        // `derive_groups` yields one record per input group, so a single-group
        // slice always has exactly one; `first()` keeps this panic-free.
        match derivation.all().first().map(|s| &s.outcome) {
            Some(clinker_channel::SelectionOutcome::Selected { .. }) => {
                matched.push(channel.id.clone())
            }
            Some(clinker_channel::SelectionOutcome::Error(e)) => {
                errors.push((channel.id.clone(), e.to_string()))
            }
            _ => {}
        }
    }
    matched.sort();

    println!("group `{}` (match: {selector})", args.group);
    if matched.is_empty() {
        println!("  members: <none>");
    } else {
        println!("  members ({}):", matched.len());
        for id in &matched {
            println!("    - {id}");
        }
    }
    if !errors.is_empty() {
        eprintln!("  selector errors ({}):", errors.len());
        for (id, reason) in &errors {
            eprintln!("    - {id}: {reason}");
        }
        return Ok(1);
    }
    Ok(0)
}

/// `clinker channels label set <key>=<value> <id...>` — stamp/overwrite one
/// label across the named channels' manifests, idempotently.
fn run_channels_label_set(args: &LabelSetArgs) -> Result<u8, Box<dyn std::error::Error>> {
    let (key, raw_value) = args
        .assignment
        .split_once('=')
        .ok_or("label assignment must be `key=value`")?;
    validate_label_key(key)?;
    let value = parse_label_value(raw_value);
    let rendered = render_label_scalar(&value);

    let workspace_root = args.base_dir.canonicalize()?;
    let clinker_toml = clinker_plan::config::ClinkerToml::load_from_workspace(&workspace_root)
        .map_err(|e| format!("clinker.toml: {e}"))?;

    let mut errors: Vec<(String, String)> = Vec::new();
    let mut changed = 0usize;
    let mut unchanged = 0usize;
    for id in &args.ids {
        let dir = clinker_channel::channel_dir(&clinker_toml.channel, &workspace_root, id);
        if !dir.is_dir() {
            errors.push((
                id.clone(),
                format!("channel folder not found ({})", dir.display()),
            ));
            continue;
        }
        let manifest_path = dir.join(clinker_channel::CHANNEL_MANIFEST_FILE);
        match set_manifest_label(&manifest_path, id, key, &value) {
            Ok(LabelOutcome::Unchanged) => {
                unchanged += 1;
                println!("{id}: {key}={rendered} (unchanged)");
            }
            Ok(outcome) => {
                changed += 1;
                let verb = match outcome {
                    LabelOutcome::Updated => "set",
                    LabelOutcome::Unchanged => unreachable!(),
                };
                println!("{id}: {verb} {key}={rendered}");
            }
            Err(e) => errors.push((id.clone(), e)),
        }
    }

    println!(
        "label set: {changed} channel(s) changed, {unchanged} already current, {} error(s)",
        errors.len()
    );
    if !errors.is_empty() {
        for (id, reason) in &errors {
            eprintln!("FAIL {id}: {reason}");
        }
        return Ok(1);
    }
    Ok(0)
}

/// The effect of a `label set` on one manifest.
#[derive(Debug)]
enum LabelOutcome {
    /// The label was added or its value changed.
    Updated,
    /// The label already had this exact value; nothing was written.
    Unchanged,
}

/// A label key must be a CXL identifier so a group selector can reference it.
fn validate_label_key(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("label key is empty".to_string());
    }
    if !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!(
            "invalid label key `{key}`: keys may only contain letters, digits, and `_` \
             (so a selector can reference them)"
        ));
    }
    Ok(())
}

/// Infer a label value's type from its text the way YAML scalar inference would,
/// so numeric/boolean labels compare correctly in selectors.
fn parse_label_value(raw: &str) -> serde_json::Value {
    match raw {
        "true" => return serde_json::Value::Bool(true),
        "false" => return serde_json::Value::Bool(false),
        _ => {}
    }
    if let Ok(i) = raw.parse::<i64>() {
        return serde_json::Value::from(i);
    }
    // Only treat as float when it round-trips and actually looks numeric (a bare
    // `inf`/`nan` parses as f64 but should stay a string label).
    if raw
        .chars()
        .next()
        .map(|c| c.is_ascii_digit() || c == '-' || c == '+' || c == '.')
        .unwrap_or(false)
        && let Ok(f) = raw.parse::<f64>()
        && f.is_finite()
        && let Some(n) = serde_json::Number::from_f64(f)
    {
        return serde_json::Value::Number(n);
    }
    serde_json::Value::String(raw.to_string())
}

/// Render a scalar label value as a YAML scalar: bare when unambiguous,
/// double-quoted when it would otherwise be misread (empty, special chars, or
/// bool/number/null-looking strings).
fn render_label_scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            if is_plain_yaml_scalar(s) {
                s.clone()
            } else {
                // Double-quoted YAML scalar with the minimal escapes.
                let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
                format!("\"{escaped}\"")
            }
        }
        // Non-scalar labels are out of the selector's scope; render defensively.
        other => other.to_string(),
    }
}

/// Whether a string can be emitted as a bare (unquoted) YAML scalar without the
/// reader reinterpreting it as another type or breaking mapping syntax.
///
/// The reader is serde-saphyr with YAML 1.1 scalar resolution, which is a
/// strictly larger recognizer than Rust's `str::parse`: it resolves booleans
/// case-insensitively (including single-letter `y`/`n`), nulls, and integers in
/// hex/octal/binary/underscore forms. This function is deliberately
/// conservative — it emits bare only for values that are unambiguously strings
/// under those rules — so a label never silently changes type on round-trip.
fn is_plain_yaml_scalar(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    // Leading/trailing whitespace is trimmed by a plain scalar; must quote.
    if s.starts_with(char::is_whitespace) || s.ends_with(char::is_whitespace) {
        return false;
    }
    // YAML 1.1 boolean / null tokens, matched case-insensitively (the reader is
    // case-insensitive and accepts the single-letter y/n forms).
    let lower = s.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "y" | "yes"
            | "true"
            | "on"
            | "n"
            | "no"
            | "false"
            | "off"
            | "null"
            | "~"
            | "nan"
            | "inf"
            | "-inf"
            | "+inf"
    ) {
        return false;
    }
    // A bare scalar that starts with anything but an ASCII letter risks being
    // read as a number (any digit / sign / dot / `0x`/`0o`/`0b` prefix start
    // here) or an indicator; require an alphabetic lead. Rust's number parser is
    // a further guard against letter-leading numerics like `inf`.
    if !s.starts_with(|c: char| c.is_ascii_alphabetic()) {
        return false;
    }
    if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
        return false;
    }
    // Keep the character set narrow and reject interior structure that would
    // change the mapping's meaning.
    s.chars()
        .all(|c| c.is_ascii_alphanumeric() || " _./-".contains(c))
        && !s.contains(": ")
        && !s.ends_with(':')
        && !s.contains(" #")
}

/// Set (or overwrite) `key` to `value` in a channel manifest, rewriting only the
/// top-level `labels:` block. Idempotent: an identical value writes nothing.
fn set_manifest_label(
    manifest_path: &std::path::Path,
    channel_id: &str,
    key: &str,
    value: &serde_json::Value,
) -> Result<LabelOutcome, String> {
    if !manifest_path.exists() {
        return Err(format!(
            "channel `{channel_id}` has no {}; create it with `channel.targets` before setting labels",
            clinker_channel::CHANNEL_MANIFEST_FILE
        ));
    }

    let original = std::fs::read_to_string(manifest_path)
        .map_err(|e| format!("reading {}: {e}", manifest_path.display()))?;
    // Parse-validate and read the current labels through the CH-2 model.
    let manifest = clinker_channel::ChannelManifest::load(manifest_path)
        .map_err(|e| format!("{}: {e}", manifest_path.display()))?;

    if manifest.labels.get(key) == Some(value) {
        return Ok(LabelOutcome::Unchanged);
    }

    let mut labels = manifest.labels.clone();
    labels.insert(key.to_string(), value.clone());
    let block = render_labels_block(&labels.into_iter().collect::<Vec<_>>());

    let new_text = splice_top_level_block(&original, "labels", &block);
    if new_text == original {
        return Ok(LabelOutcome::Unchanged);
    }
    std::fs::write(manifest_path, new_text)
        .map_err(|e| format!("writing {}: {e}", manifest_path.display()))?;
    Ok(LabelOutcome::Updated)
}

/// Render a `labels:` block as YAML lines (block style, deterministic order).
fn render_labels_block(labels: &[(String, serde_json::Value)]) -> Vec<String> {
    let mut lines = vec!["labels:".to_string()];
    for (k, v) in labels {
        lines.push(format!("  {k}: {}", render_label_scalar(v)));
    }
    lines
}

/// Replace the top-level `<key>:` block (that line plus its indented body) with
/// `block`, or insert `block` (after a `channel:` block if present, else at end)
/// when the key is absent. Preserves every other line, including comments.
fn splice_top_level_block(text: &str, key: &str, block: &[String]) -> String {
    let had_trailing_newline = text.ends_with('\n');
    // Preserve the file's line ending: `lines()` strips `\r`, so re-join with the
    // same terminator the manifest already used rather than forcing LF.
    let newline = if text.contains("\r\n") { "\r\n" } else { "\n" };
    let lines: Vec<&str> = text.lines().collect();

    let key_line = lines.iter().position(|l| top_level_key(l) == Some(key));

    let mut out: Vec<String> = Vec::new();
    match key_line {
        Some(start) => {
            let end = block_extent(&lines, start);
            out.extend(lines[..start].iter().map(|s| s.to_string()));
            out.extend(block.iter().cloned());
            out.extend(lines[end..].iter().map(|s| s.to_string()));
        }
        None => {
            // Insert after the `channel:` block if there is one, else at the end.
            let insert_at = match lines
                .iter()
                .position(|l| top_level_key(l) == Some("channel"))
            {
                Some(c) => block_extent(&lines, c),
                None => lines.len(),
            };
            out.extend(lines[..insert_at].iter().map(|s| s.to_string()));
            out.extend(block.iter().cloned());
            out.extend(lines[insert_at..].iter().map(|s| s.to_string()));
        }
    }

    let mut joined = out.join(newline);
    if had_trailing_newline {
        joined.push_str(newline);
    }
    joined
}

/// The extent of a top-level block: the key line at `start` up to (excluding)
/// the next top-level mapping key, or end of input.
///
/// The block ends at the next *key* — not merely the next column-0 line — so a
/// column-0 comment or blank interleaved between indented body lines stays
/// inside the block rather than splitting it (which would orphan the body lines
/// after it into invalid YAML). The cost is that a comment sitting inside the
/// replaced block is dropped; for the `labels:` rewrite this is a bounded,
/// block-local formatting loss, never a correctness hazard.
fn block_extent(lines: &[&str], start: usize) -> usize {
    let mut end = start + 1;
    while end < lines.len() && top_level_key(lines[end]).is_none() {
        end += 1;
    }
    end
}

/// The key name of a top-level (column-0) mapping entry line, or `None` for
/// indented lines, comments, blanks, and list items.
fn top_level_key(line: &str) -> Option<&str> {
    let first = line.chars().next()?;
    if first.is_whitespace() || first == '#' || first == '-' {
        return None;
    }
    let key = line.split_once(':')?.0;
    if key.is_empty() || key.contains(' ') {
        return None;
    }
    Some(key)
}

/// Build a closure that renders channel/group scan diagnostics into a message.
fn diag_message(
    prefix: &'static str,
) -> impl FnOnce(Vec<clinker_core_types::Diagnostic>) -> String {
    move |diags| {
        let msgs: Vec<String> = diags
            .iter()
            .map(|d| format!("[{}] {}", d.code, d.message))
            .collect();
        format!("{prefix}: {}", msgs.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_dispatch_mismatch() {
        use clinker_core_types::{FailureCategory, RetryAdvice};

        let error = PipelineError::DispatchMismatch {
            dispatcher: "dispatch_route",
            expected_kind: "route",
            actual_kind: "transform",
            node: "normalize_orders".to_owned(),
        };

        let classification = classify_pipeline_error(&error);
        assert_eq!(classification, error.failure_classification().unwrap());
        assert_eq!(classification.code(), "runtime.invariant.dispatch_mismatch");
        assert_eq!(classification.category(), FailureCategory::InternalInvariant);
        assert_eq!(classification.retry_advice(), RetryAdvice::PolicyRequired);
    }

    #[test]
    fn runtime_failure_classification_distinguishes_policy_from_transience() {
        use clinker_core_types::RetryAdvice;
        use clinker_plan::runtime_error::{BudgetCategory, SpillError};

        let cases = [
            (
                PipelineError::MemoryBudgetExceeded {
                    node: "aggregate".to_owned(),
                    used: 2,
                    limit: 1,
                    source: BudgetCategory::Arena,
                    detail: None,
                },
                "runtime.resource.memory_budget_exceeded",
                RetryAdvice::PolicyRequired,
            ),
            (
                PipelineError::UnsatisfiableMemoryBudget {
                    limit: 1,
                    baseline_rss: 2,
                },
                "admission.configuration.memory_budget_unsatisfiable",
                RetryAdvice::DoNotRetry,
            ),
            (
                PipelineError::SpillCapExceeded {
                    node: "sort".to_owned(),
                    cap: 1,
                    attempted: 2,
                    current: 2,
                },
                "runtime.resource.spill_cap_exceeded",
                RetryAdvice::PolicyRequired,
            ),
            (
                PipelineError::Spill(SpillError::Io(std::io::Error::other("closed"))),
                "runtime.resource.spill_failed",
                RetryAdvice::RetryWithBackoff,
            ),
        ];

        for (error, expected_code, expected_retry) in cases {
            let classification = classify_pipeline_error(&error);
            assert_eq!(classification.code(), expected_code);
            assert_eq!(classification.retry_advice(), expected_retry);
        }
    }

    #[test]
    fn multiple_failure_classification_is_order_independent_and_fail_closed() {
        let classify = |errors| classify_pipeline_error(&PipelineError::Multiple(errors));
        let first = classify(vec![
            PipelineError::Io(std::io::Error::other("temporary")),
            PipelineError::SpillCapExceeded {
                node: "sort".to_owned(),
                cap: 1,
                attempted: 2,
                current: 2,
            },
            PipelineError::SortOrderViolation {
                message: "out of order".to_owned(),
            },
        ]);
        let reversed = classify(vec![
            PipelineError::SortOrderViolation {
                message: "out of order".to_owned(),
            },
            PipelineError::SpillCapExceeded {
                node: "sort".to_owned(),
                cap: 1,
                attempted: 2,
                current: 2,
            },
            PipelineError::Io(std::io::Error::other("temporary")),
        ]);

        assert_eq!(first.code(), "source.data.invalid");
        assert_eq!(first, reversed);
    }

    #[test]
    fn publication_failure_diagnostics_are_path_safe_and_actionable() {
        let execution_id = "018f47a2-9a41-7a27-b4d6-4f7137e3c273";
        for failure in [
            PublicationFailureKind::Readiness,
            PublicationFailureKind::ReadinessAndAbandonment,
            PublicationFailureKind::CleanupDebt(2),
            PublicationFailureKind::Incomplete(1),
            PublicationFailureKind::Publish("I/O operation".to_owned()),
        ] {
            let rendered = publication_failure_diagnostic(execution_id, failure);
            assert!(rendered.contains(execution_id), "{rendered}");
            assert!(rendered.contains("clinker attempts inspect"), "{rendered}");
            assert!(!rendered.contains("/tmp/"), "{rendered}");
            assert!(!rendered.contains("\\Users\\"), "{rendered}");
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_argument_quoting_round_trips_through_cmd() {
        use std::os::windows::process::CommandExt;

        let expected = [
            "argument with spaces",
            r#"say "hello""#,
            r"C:\",
            r"C:\Program Files\",
            "",
        ];
        let output_dir = tempfile::tempdir().expect("temporary output directory");
        let output_path = output_dir.path().join("argv.json");
        let mut argv = vec![
            std::env::current_exe()
                .expect("current test executable")
                .into_os_string()
                .into_string()
                .expect("test executable path should be Unicode"),
            "--exact".to_owned(),
            "tests::windows_argument_probe".to_owned(),
            "--ignored".to_owned(),
            "--".to_owned(),
        ];
        argv.extend(expected.iter().map(|value| (*value).to_owned()));

        let command = render_attempt_command(&argv);
        let mut process = std::process::Command::new("cmd.exe");
        process.args(["/D", "/S", "/C"]);
        // cmd.exe requires one additional pair of quotes around the complete
        // raw command after `/C`; `raw_arg` avoids Command::arg quoting that
        // wrapper a second time.
        process.raw_arg(format!("\"{command}\""));
        let output = process
            .env("CLINKER_ARGV_PROBE_OUTPUT", &output_path)
            .output()
            .expect("cmd.exe should launch the argv probe");
        assert!(
            output.status.success(),
            "argv probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let actual: Vec<String> = serde_json::from_slice(
            &std::fs::read(output_path).expect("argv probe should write its captured arguments"),
        )
        .expect("argv probe output should be JSON");
        assert_eq!(actual, expected);
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "launched by windows_argument_quoting_round_trips_through_cmd"]
    fn windows_argument_probe() {
        let output_path = std::env::var_os("CLINKER_ARGV_PROBE_OUTPUT")
            .expect("argv probe output path should be provided");
        let actual = std::env::args()
            .skip_while(|argument| argument != "--")
            .skip(1)
            .collect::<Vec<_>>();
        std::fs::write(
            output_path,
            serde_json::to_vec(&actual).expect("captured arguments should serialize"),
        )
        .expect("captured arguments should be written");
    }

    #[test]
    fn windows_argument_quoting_preserves_quotes_and_trailing_backslashes() {
        assert_eq!(quote_windows_argument(""), "\"\"");
        assert_eq!(
            quote_windows_argument(r#"C:\Program Files\"#),
            "\"C:\\Program Files\\\\\""
        );
        assert_eq!(
            quote_windows_argument(r#"say "hello"\"#),
            "\"say \\\"hello\\\"\\\\\""
        );
    }

    #[test]
    fn line_byte_range_handles_lf_crlf_and_lone_cr() {
        for text in ["one\ntwo\nthree", "one\r\ntwo\r\nthree", "one\rtwo\rthree"] {
            assert_eq!(line_byte_range(text, 1), Some((0, 3)));
            assert_eq!(&text[line_byte_range(text, 2).unwrap().0..][..3], "two");
            assert_eq!(&text[line_byte_range(text, 3).unwrap().0..][..5], "three");
            assert_eq!(line_byte_range(text, 4), None);
        }
    }
    use clap::Parser;

    #[test]
    fn declared_type_threshold_uses_the_data_quality_exit_code() {
        let declared_type_threshold = PipelineError::TypeErrorThresholdExceeded {
            observed_rate: 0.25,
            max_rate: 0.20,
            observed_count: 1,
            total_count: 4,
        };
        let dlq_rate_threshold = PipelineError::DlqRateExceeded {
            source: Some(std::sync::Arc::from("input")),
            observed_rate: 0.25,
            max_rate: 0.20,
            observed_count: 1,
            total_count: 4,
        };

        assert_eq!(pipeline_error_exit_code(&declared_type_threshold), 3);
        assert_eq!(
            pipeline_error_exit_code(&declared_type_threshold),
            pipeline_error_exit_code(&dlq_rate_threshold),
        );
    }

    // ── Plan-diagnostic rendering ───────────────────────────────────────

    #[test]
    fn line_byte_range_excludes_the_line_terminator() {
        let lf = "alpha\nbravo\ncharlie\n";
        assert_eq!(line_byte_range(lf, 1), Some((0, 5)));
        assert_eq!(line_byte_range(lf, 2), Some((6, 5)));
        assert_eq!(line_byte_range(lf, 3), Some((12, 7)));
        // Past the last line there is nothing to underline.
        assert_eq!(line_byte_range(lf, 9), None);
    }

    #[test]
    fn line_byte_range_excludes_a_carriage_return() {
        // A YAML saved on Windows ends every line with CRLF. Counting the
        // `\r` would run the underline one column past the last visible
        // character of the line.
        let crlf = "alpha\r\nbravo\r\ncharlie\r\n";
        assert_eq!(line_byte_range(crlf, 1), Some((0, 5)));
        assert_eq!(line_byte_range(crlf, 2), Some((7, 5)));
        assert_eq!(line_byte_range(crlf, 3), Some((14, 7)));
        // A final line with no terminator at all still measures correctly.
        assert_eq!(line_byte_range("only", 1), Some((0, 4)));
        assert_eq!(line_byte_range("only\r", 1), Some((0, 4)));
    }

    #[test]
    fn severity_maps_through_rather_than_flattening_to_error() {
        use clinker_core_types::Severity;
        assert!(matches!(
            miette_severity(Severity::Error),
            miette::Severity::Error
        ));
        assert!(matches!(
            miette_severity(Severity::Warning),
            miette::Severity::Warning
        ));
        assert!(matches!(
            miette_severity(Severity::Note),
            miette::Severity::Advice
        ));
    }

    // ── CH-15: label-value inference / rendering ────────────────────────

    #[test]
    fn label_value_inference_typing() {
        assert_eq!(parse_label_value("true"), serde_json::Value::Bool(true));
        assert_eq!(parse_label_value("false"), serde_json::Value::Bool(false));
        assert_eq!(parse_label_value("3"), serde_json::Value::from(3i64));
        assert_eq!(
            parse_label_value("enterprise"),
            serde_json::Value::from("enterprise")
        );
        assert!(parse_label_value("0.9").is_number());
        // Non-numeric leading char stays a string even if f64 could parse it.
        assert_eq!(parse_label_value("inf"), serde_json::Value::from("inf"));
    }

    #[test]
    fn label_scalar_rendering_quotes_only_when_needed() {
        assert_eq!(
            render_label_scalar(&serde_json::Value::from("west")),
            "west"
        );
        assert_eq!(render_label_scalar(&serde_json::Value::from(3i64)), "3");
        assert_eq!(render_label_scalar(&serde_json::Value::Bool(true)), "true");
        // A string that would otherwise be read as a bool/number must be quoted.
        assert_eq!(
            render_label_scalar(&serde_json::Value::from("true")),
            "\"true\""
        );
        assert_eq!(render_label_scalar(&serde_json::Value::from("3")), "\"3\"");
        assert_eq!(
            render_label_scalar(&serde_json::Value::from("a: b")),
            "\"a: b\""
        );
        // YAML 1.1 booleans/nulls the reader resolves case-insensitively, and the
        // single-letter forms, must be quoted so they stay strings on reload.
        for s in [
            "True", "TRUE", "Yes", "No", "OFF", "Null", "y", "Y", "n", "N", "~",
        ] {
            let q = render_label_scalar(&serde_json::Value::from(s));
            assert!(q.starts_with('"'), "{s} must be quoted, got {q}");
        }
        // YAML numeric forms Rust's parser misses (hex/oct/bin/underscore) must
        // also quote so they are not re-read as ints/floats.
        for s in ["0x1f", "0o17", "0b101", "1_000", ".inf"] {
            let q = render_label_scalar(&serde_json::Value::from(s));
            assert!(q.starts_with('"'), "{s} must be quoted, got {q}");
        }
        // Leading/trailing whitespace would be trimmed by a bare scalar.
        assert_eq!(
            render_label_scalar(&serde_json::Value::from(" west")),
            "\" west\""
        );
        // Ordinary string labels stay bare, including interior punctuation.
        assert_eq!(
            render_label_scalar(&serde_json::Value::from("west-coast")),
            "west-coast"
        );
    }

    #[test]
    fn label_key_validation() {
        assert!(validate_label_key("tier").is_ok());
        assert!(validate_label_key("region_west").is_ok());
        assert!(validate_label_key("").is_err());
        assert!(validate_label_key("has space").is_err());
        assert!(validate_label_key("dotted.key").is_err());
    }

    // ── CH-15: label set is idempotent and format-preserving ─────────────

    fn write_channel_manifest(root: &std::path::Path, id: &str, body: &str) -> std::path::PathBuf {
        let dir = root.join("channel").join(id);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(clinker_channel::CHANNEL_MANIFEST_FILE);
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn label_set_inserts_updates_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // Manifest with a comment and existing config that must survive.
        let path = write_channel_manifest(
            root,
            "acme",
            "channel:\n  name: acme\n  targets: [sales.orders]\n# keep me\nlabels:\n  region: west\nconfig:\n  fraud.threshold: { value: 0.9 }\n",
        );

        // Insert a new label.
        let out = set_manifest_label(
            &path,
            "acme",
            "tier",
            &serde_json::Value::from("enterprise"),
        )
        .unwrap();
        assert!(matches!(out, LabelOutcome::Updated));
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("# keep me"), "comment preserved:\n{text}");
        assert!(
            text.contains("fraud.threshold: { value: 0.9 }"),
            "config preserved:\n{text}"
        );
        assert!(
            text.contains("region: west") && text.contains("tier: enterprise"),
            "{text}"
        );

        // Re-running with the same value is a no-op.
        let out = set_manifest_label(
            &path,
            "acme",
            "tier",
            &serde_json::Value::from("enterprise"),
        )
        .unwrap();
        assert!(matches!(out, LabelOutcome::Unchanged));
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            text,
            "idempotent write"
        );

        // Overwriting an existing label value updates it.
        let out =
            set_manifest_label(&path, "acme", "region", &serde_json::Value::from("east")).unwrap();
        assert!(matches!(out, LabelOutcome::Updated));
        let m = clinker_channel::ChannelManifest::load(&path).unwrap();
        assert_eq!(
            m.labels.get("region"),
            Some(&serde_json::Value::from("east"))
        );
    }

    #[test]
    fn label_set_with_interleaved_comment_stays_valid_yaml() {
        // A column-0 comment sitting *between* indented label lines must not
        // orphan the labels after it — the rebuilt block absorbs the region and
        // the result re-parses with every label present and `config:` intact.
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let path = write_channel_manifest(
            root,
            "acme",
            "channel:\n  name: acme\n  targets: [sales.orders]\nlabels:\n  region: west\n# note\n  region2: east\nconfig:\n  k.v: { value: 1 }\n",
        );
        set_manifest_label(
            &path,
            "acme",
            "tier",
            &serde_json::Value::from("enterprise"),
        )
        .unwrap();
        let m = clinker_channel::ChannelManifest::load(&path).unwrap();
        assert_eq!(
            m.labels.get("region"),
            Some(&serde_json::Value::from("west"))
        );
        assert_eq!(
            m.labels.get("region2"),
            Some(&serde_json::Value::from("east"))
        );
        assert_eq!(
            m.labels.get("tier"),
            Some(&serde_json::Value::from("enterprise"))
        );
        assert!(m.config.contains_key("k.v"), "config survives");
    }

    #[test]
    fn label_set_rejects_absent_manifest_without_writing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let dir = root.join("channel").join("globex");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(clinker_channel::CHANNEL_MANIFEST_FILE);

        let error = set_manifest_label(
            &path,
            "globex",
            "tier",
            &serde_json::Value::from("enterprise"),
        )
        .expect_err("targetless manifests must not be created");
        assert!(error.contains("channel.targets"), "{error}");
        assert!(!path.exists(), "an invalid manifest must not be written");
    }

    // ── CH-15: group members via the full command ───────────────────────

    #[test]
    fn group_members_lists_selector_matches() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(
            root.join("clinker.toml"),
            "[channel]\nroot=\"channel\"\n[group]\nroot=\"group\"\n",
        )
        .unwrap();
        std::fs::create_dir_all(root.join("group")).unwrap();
        std::fs::write(
            root.join("group/enterprise.group.yaml"),
            "group:\n  name: enterprise\n  targets: { pipelines: [sales.orders] }\n  match: 'tier == \"enterprise\"'\n",
        )
        .unwrap();
        write_channel_manifest(
            root,
            "acme",
            "channel:\n  name: acme\n  targets: [sales.orders]\nlabels:\n  tier: enterprise\n",
        );
        write_channel_manifest(
            root,
            "beta",
            "channel:\n  name: beta\n  targets: [sales.orders]\nlabels:\n  tier: basic\n",
        );

        let code = run_channels_group_members(&GroupMembersArgs {
            group: "enterprise".to_string(),
            base_dir: root.to_path_buf(),
        })
        .unwrap();
        assert_eq!(code, 0, "acme matches, beta does not, no selector errors");
    }

    /// The compile anchor must reconstruct the pipeline file's directory —
    /// the same directory the runtime source-discovery layer anchors on — and
    /// must NOT collapse to the process CWD. Here the pipeline lives in a temp
    /// directory that is not the CWD, so an anchor equal to the CWD would mean
    /// compile-time source-size estimates name different files than the run
    /// reads.
    #[test]
    fn compile_anchor_reconstructs_pipeline_dir_not_cwd() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let pdir = tmp.path().canonicalize().expect("canonicalize tmp");
        let pipeline = pdir.join("pipeline.yaml");
        std::fs::write(&pipeline, "pipeline:\n  name: x\n").expect("write pipeline");

        let (workspace_root, pipeline_dir) = resolve_compile_anchor(&pipeline, None);
        assert_eq!(
            workspace_root.join(&pipeline_dir),
            pdir,
            "anchor must reconstruct the pipeline file's directory"
        );
        let cwd = std::env::current_dir()
            .ok()
            .and_then(|c| c.canonicalize().ok());
        assert_ne!(
            Some(workspace_root.join(&pipeline_dir)),
            cwd,
            "the temp pipeline dir is not the CWD; the anchor must not collapse to the CWD"
        );
        assert_eq!(
            pipeline_dir,
            PathBuf::new(),
            "with no --base-dir the pipeline lives at the workspace root"
        );
    }

    /// With `--base-dir` set to an ancestor of the pipeline file, the join of
    /// workspace_root + pipeline_dir must still reconstruct the pipeline
    /// file's directory (the runtime discovery anchor), with pipeline_dir the
    /// relative offset.
    #[test]
    fn compile_anchor_honors_base_dir_ancestor() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().canonicalize().expect("canonicalize tmp");
        let sub = root.join("sub").join("nested");
        std::fs::create_dir_all(&sub).expect("mkdir nested");
        let pipeline = sub.join("pipeline.yaml");
        std::fs::write(&pipeline, "pipeline:\n  name: x\n").expect("write pipeline");

        let (workspace_root, pipeline_dir) = resolve_compile_anchor(&pipeline, Some(&root));
        assert_eq!(workspace_root, root, "workspace root is the --base-dir");
        assert_eq!(
            pipeline_dir,
            PathBuf::from("sub").join("nested"),
            "pipeline_dir is the offset from base-dir to the pipeline directory"
        );
        assert_eq!(
            workspace_root.join(&pipeline_dir),
            sub,
            "join must reconstruct the pipeline file's directory"
        );
    }

    #[test]
    fn test_cli_run_positional_config_path() {
        let cli = Cli::try_parse_from(["clinker", "run", "pipeline.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.config, PathBuf::from("pipeline.yaml")),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_dry_run_flag() {
        let cli = Cli::try_parse_from(["clinker", "run", "--dry-run", "pipeline.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert!(args.dry_run),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_log_level_default() {
        let cli = Cli::try_parse_from(["clinker", "run", "pipeline.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.log_level, "info"),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_suffix_k() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--memory-limit", "512K", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert_eq!(
                    args.resolved_memory_limit().unwrap(),
                    Some("512K".to_string())
                )
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_suffix_m() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--memory-limit", "256M", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert_eq!(
                    args.resolved_memory_limit().unwrap(),
                    Some("256M".to_string())
                )
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_suffix_g() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--memory-limit", "2G", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert_eq!(
                    args.resolved_memory_limit().unwrap(),
                    Some("2G".to_string())
                )
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_bare_bytes() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--memory-limit", "1000000", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert_eq!(
                    args.resolved_memory_limit().unwrap(),
                    Some("1000000".to_string())
                )
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_absent_resolves_to_none() {
        // An omitted flag resolves to `None`, so the caller leaves the YAML
        // budget (and the arbitrator's own default) untouched.
        let cli = Cli::try_parse_from(["clinker", "run", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.resolved_memory_limit().unwrap(), None),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_empty_treated_as_absent() {
        // An ops wrapper like `clinker run --memory-limit "$CLINKER_MEM" ...`
        // expands an unset variable to `--memory-limit ""`. An empty or
        // whitespace-only value must resolve to `None` — falling back to the YAML
        // budget exactly as an omitted flag would — never a hard abort.
        for empty in ["", "   "] {
            let cli =
                Cli::try_parse_from(["clinker", "run", "--memory-limit", empty, "p.yaml"]).unwrap();
            match cli.command {
                Commands::Run(args) => assert_eq!(
                    args.resolved_memory_limit().unwrap(),
                    None,
                    "empty flag value {empty:?} must resolve to None"
                ),
                _ => panic!("expected Run command"),
            }
        }
    }

    #[test]
    fn test_cli_run_memory_limit_malformed_fails_naming_flag() {
        // A malformed value (the decimal `4GB` where the binary `4G` is meant,
        // or plain garbage) must be rejected at the boundary — naming the flag
        // and echoing the value — never coerced to the default, which would
        // silently shrink a larger configured budget.
        for bad in ["4GB", "notanumber"] {
            let cli =
                Cli::try_parse_from(["clinker", "run", "--memory-limit", bad, "p.yaml"]).unwrap();
            match cli.command {
                Commands::Run(args) => match args.resolved_memory_limit() {
                    Err(clinker_plan::config::ConfigError::Validation(msg)) => {
                        assert!(
                            msg.contains("--memory-limit") && msg.contains(bad),
                            "diagnostic must name the flag and echo {bad:?}; got: {msg}"
                        );
                    }
                    other => panic!("expected a validation error for {bad:?}, got: {other:?}"),
                },
                _ => panic!("expected Run command"),
            }
        }
    }

    #[test]
    fn test_cli_run_memory_limit_overflow_fails_naming_flag() {
        // A well-formed but oversized value (numeric part fits, suffix multiply
        // overflows `u64`) must also fail naming the flag the operator passed,
        // not the YAML `memory.limit` key.
        let cli =
            Cli::try_parse_from(["clinker", "run", "--memory-limit", "17179869184G", "p.yaml"])
                .unwrap();
        match cli.command {
            Commands::Run(args) => match args.resolved_memory_limit() {
                Err(clinker_plan::config::ConfigError::Validation(msg)) => {
                    assert!(
                        msg.contains("--memory-limit") && msg.contains("overflow"),
                        "diagnostic must name the flag and the overflow; got: {msg}"
                    );
                }
                other => panic!("expected a validation error, got: {other:?}"),
            },
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_error_threshold_zero() {
        let cli = Cli::try_parse_from(["clinker", "run", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.error_threshold, 0),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_batch_id_default_uuid() {
        let cli = Cli::try_parse_from(["clinker", "run", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert!(args.batch_id.is_none());
                let id = args.resolved_batch_id();
                uuid::Uuid::parse_str(&id).expect("default batch_id should be valid UUID");
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_quiet_flag() {
        let cli = Cli::try_parse_from(["clinker", "run", "--quiet", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert!(args.quiet),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_force_flag() {
        let cli = Cli::try_parse_from(["clinker", "run", "--force", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert!(args.force),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_metrics_spool_dir_flag() {
        let cli = Cli::try_parse_from([
            "clinker",
            "run",
            "--metrics-spool-dir",
            "/var/spool/clinker",
            "p.yaml",
        ])
        .unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert_eq!(
                    args.metrics_spool_dir,
                    Some(PathBuf::from("/var/spool/clinker"))
                );
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_metrics_collect_parses() {
        let cli = Cli::try_parse_from([
            "clinker",
            "metrics",
            "collect",
            "--spool-dir",
            "/var/spool/clinker",
            "--output-file",
            "/data/metrics.ndjson",
            "--delete-after-collect",
        ])
        .unwrap();
        match cli.command {
            Commands::Metrics {
                subcommand: MetricsCommands::Collect(args),
            } => {
                assert_eq!(args.spool_dir, PathBuf::from("/var/spool/clinker"));
                assert_eq!(args.output_file, PathBuf::from("/data/metrics.ndjson"));
                assert!(args.delete_after_collect);
                assert!(!args.dry_run);
            }
            _ => panic!("expected Metrics::Collect command"),
        }
    }

    #[test]
    fn test_cli_metrics_collect_dry_run() {
        let cli = Cli::try_parse_from([
            "clinker",
            "metrics",
            "collect",
            "--spool-dir",
            "/spool",
            "--output-file",
            "/out.ndjson",
            "--dry-run",
        ])
        .unwrap();
        match cli.command {
            Commands::Metrics {
                subcommand: MetricsCommands::Collect(args),
            } => {
                assert!(args.dry_run);
            }
            _ => panic!("expected Metrics::Collect command"),
        }
    }

    #[test]
    fn test_cli_run_env_flag() {
        let cli = Cli::try_parse_from(["clinker", "run", "--env", "prod", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.env, Some("prod".into())),
            _ => panic!("expected Run command"),
        }
    }

    // ── Dry-run -n CLI tests ──────────────────────────────────────

    #[test]
    fn test_dry_run_n_flag() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--dry-run", "-n", "10", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert!(args.dry_run);
                assert_eq!(args.dry_run_n, Some(10));
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_dry_run_output_flag() {
        let cli = Cli::try_parse_from([
            "clinker",
            "run",
            "--dry-run",
            "-n",
            "5",
            "--dry-run-output",
            "out.csv",
            "p.yaml",
        ])
        .unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert!(args.dry_run);
                assert_eq!(args.dry_run_n, Some(5));
                assert_eq!(args.dry_run_output, Some(PathBuf::from("out.csv")));
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_dry_run_without_n_config_only() {
        let cli = Cli::try_parse_from(["clinker", "run", "--dry-run", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert!(args.dry_run);
                assert!(args.dry_run_n.is_none());
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_dry_run_default_stdout() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--dry-run", "-n", "3", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert!(args.dry_run_output.is_none()); // default to stdout
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn cap_headroom_explain_states_per_invocation_and_warns_over_threshold() {
        // 9 GB estimate vs a 10 GB cap is 90%, over the 80% threshold → warning
        // line, plus the per-invocation disclaimer (#311).
        let out = cap_headroom_explain(Some(10_000_000_000), 9_000_000_000);
        assert!(
            out.contains("Cap headroom:"),
            "must render headroom line: {out}"
        );
        assert!(
            out.contains("per invocation") && out.contains("sibling invocations"),
            "must disclaim sibling invocations sharing the volume: {out}"
        );
        assert!(
            out.contains("WARNING"),
            "90% of cap must emit a warning: {out}"
        );

        // 50% of the cap is under threshold → headroom line, no WARNING.
        let ok = cap_headroom_explain(Some(10_000_000_000), 5_000_000_000);
        assert!(ok.contains("Cap headroom:"));
        assert!(!ok.contains("WARNING"), "50% of cap must not warn: {ok}");

        // No cap configured → nothing rendered (unlimited spill has no headroom).
        assert!(cap_headroom_explain(None, 5_000_000_000).is_empty());
    }

    #[test]
    fn staging_plan_explain_reports_disabled_in_place() {
        let config: clinker_plan::config::PipelineConfig = clinker_plan::config::parse_config(
            r#"
pipeline:
  name: x
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: a, type: string }
  - type: output
    name: out
    input: orders
    config:
      name: out
      type: csv
      path: out.csv
"#,
        )
        .expect("parse");
        let policy = clinker_plan::config::StagingPolicy::default();
        let out = staging_plan_explain(&config, &policy, std::path::Path::new("."));
        assert!(out.contains("=== Staging Plan ==="));
        assert!(
            out.contains("Source staging is disabled"),
            "disabled policy must say every source reads in place: {out}"
        );
    }

    #[test]
    fn staging_plan_explain_reports_staged_path_for_matched_source() {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::write(tmp.path().join("orders.csv"), b"a\n1\n").expect("write source");
        let stage_dir = tempfile::tempdir().expect("stage dir");
        let config: clinker_plan::config::PipelineConfig = clinker_plan::config::parse_config(
            r#"
pipeline:
  name: x
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: a, type: string }
  - type: output
    name: out
    input: orders
    config:
      name: out
      type: csv
      path: out.csv
"#,
        )
        .expect("parse");
        let policy = clinker_plan::config::StagingPolicy {
            enabled: true,
            dir: Some(stage_dir.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        let out = staging_plan_explain(&config, &policy, tmp.path());
        assert!(out.contains("=== Staging Plan ==="));
        assert!(
            out.contains("Source 'orders':") && out.contains("staged: yes"),
            "a matched source must report staged: yes with its path: {out}"
        );
        assert!(
            out.contains(".staged"),
            "the resolved staged path must appear: {out}"
        );
    }
}
