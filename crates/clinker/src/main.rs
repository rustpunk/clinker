use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};

use clinker_exec::executor::PipelineExecutor;
use clinker_exec::metrics::{self, ExecutionMetrics};
use clinker_plan::config::utils::parse_memory_limit_bytes;
use clinker_plan::error::PipelineError;

mod refactor;

/// CXL streaming ETL engine.
#[derive(Parser, Debug)]
#[command(
    name = "clinker",
    version,
    about = "CXL streaming ETL engine",
    long_about = "\
Clinker is a streaming ETL engine that reads tabular data (CSV, NDJSON), \
applies CXL transformation expressions, and writes the results to output files.\n\n\
Pipelines are defined in YAML configuration files that specify inputs, outputs, \
field mappings with CXL expressions, and optional channel overrides for \
multi-tenant customization.",
    after_long_help = "\
QUICK START:
  clinker run pipeline.yaml
  clinker run pipeline.yaml --dry-run -n 10
  clinker run pipeline.yaml --explain
  clinker run pipeline.yaml --channel acme-corp

ENVIRONMENT VARIABLES:
  CLINKER_ENV                   Active environment for when: conditions
  CLINKER_METRICS_SPOOL_DIR     Default metrics spool directory

EXIT CODES:
  0  Success
  1  Configuration, schema, or CXL compilation error
  2  Pipeline completed but DLQ entries were produced
  3  CXL evaluation error
  4  I/O or format error"
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
    /// Explain pipeline field provenance or error codes
    #[command(
        long_about = "\
Inspect field-level provenance chains or look up error/warning code documentation.\n\n\
Use --field to trace where a resolved value comes from across all configuration \
layers. A two-part `node.param` path traces a composition config value \
(composition defaults, channel defaults, channel fixed); a three-part \
`source.column.attribute` path traces a source-schema attribute across the \
Base < Pipeline < Group < Channel schema layers. \
Use --code to look up the documentation for a diagnostic code (composition codes \
E101–E108, combine codes E300-E319 and W302/W305/W306, memory codes E310-E312, \
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
  clinker explain --code E105"
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
    #[arg(long, default_value = "./rules/", help_heading = "Paths")]
    pub rules_path: PathBuf,

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
    /// Resolve memory limit from CLI flag or default (512MB).
    ///
    /// Returns a config error when the flag value's binary-suffix scaling
    /// overflows `u64`, so the caller surfaces a clear diagnostic to the
    /// operator instead of panicking or silently wrapping to a tiny budget.
    pub fn memory_limit_bytes(&self) -> Result<u64, clinker_plan::config::ConfigError> {
        parse_memory_limit_bytes(self.mem_limit.as_deref().or(Some("512M")))
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

    /// Dotted field path to explain provenance for (e.g. "enrich1.fuzzy_threshold")
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

    /// Error/warning code to look up (e.g. "E105")
    #[arg(long)]
    pub code: Option<String>,

    /// Base directory for relative path resolution
    #[arg(long, default_value = ".")]
    pub base_dir: PathBuf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Run(args) => {
            let filter = args
                .log_level
                .parse::<tracing_subscriber::filter::LevelFilter>()
                .unwrap_or(tracing_subscriber::filter::LevelFilter::INFO);
            tracing_subscriber::fmt().with_max_level(filter).init();

            // Install the process-wide SIGINT/SIGTERM handler before the
            // run starts so an interrupt during a long pipeline trips the
            // run's shutdown token. Idempotent — the first call wins.
            if let Err(e) = clinker_exec::pipeline::shutdown::install_signal_handler() {
                eprintln!("clinker: failed to install signal handler: {e}");
            }

            // The executor is fully synchronous — call it directly.
            match run(args) {
                Ok(code) => ExitCode::from(code),
                Err(e) => {
                    render_pipeline_error(&e, &args.config);
                    match &e {
                        PipelineError::Config(_)
                        | PipelineError::Schema(_)
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
                        | PipelineError::EnvelopeMultiHeaderConflict { .. }
                        | PipelineError::EnvelopeHeaderGrainUnmatched { .. }
                        | PipelineError::EnvelopeHeaderMultipleForGrain { .. } => ExitCode::from(1),
                        // Disk-cap exceedance (E320) is a resource-exhaustion
                        // halt — the run filled its configured spill budget.
                        // Group it with the other infrastructure failures
                        // (I/O, spill, full-volume) at exit 4 rather than the
                        // config exit 1: the pipeline is valid, the host ran
                        // out of the disk headroom the cap allotted.
                        PipelineError::Io(_)
                        | PipelineError::Spill(_)
                        | PipelineError::SpillCapExceeded { .. } => ExitCode::from(4),
                        PipelineError::Eval(_) | PipelineError::Accumulator { .. } => {
                            ExitCode::from(3)
                        }
                        PipelineError::Format(_)
                        | PipelineError::ThreadPool(_)
                        | PipelineError::Multiple(_) => ExitCode::from(4),
                        // Diagnostic-carrier — never propagated as a
                        // top-level error; folded into DLQ at the
                        // emission site. Treat as exit 4 defensively
                        // in case a future caller surfaces it.
                        PipelineError::CorrelationGroupOverflow { .. } => ExitCode::from(4),
                        // Configured DLQ rate ceiling tripped (E315 /
                        // E316). Treat as a data-quality halt — exit 3
                        // sits between config (1) and infrastructure (4).
                        PipelineError::DlqRateExceeded { .. } => ExitCode::from(3),
                        // A shutdown signal unwound the run before it
                        // finished draining. 130 is the conventional
                        // "terminated by SIGINT" status.
                        PipelineError::Interrupted => ExitCode::from(130),
                    }
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
        Commands::Explain(args) => {
            tracing_subscriber::fmt()
                .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
                .init();
            match run_explain(args) {
                Ok(()) => ExitCode::SUCCESS,
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
    }
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
fn render_pipeline_error(err: &PipelineError, config_path: &std::path::Path) {
    use miette::{Diagnostic, NamedSource, Report, Severity};
    use std::fmt;

    // Best-effort source attach. If the config file is unreadable
    // we still render the raw error via miette's graphical handler
    // so the user sees consistent formatting.
    let source_text = std::fs::read_to_string(config_path).ok();
    let filename = config_path.to_string_lossy().into_owned();

    /// Hand-rolled `Error + Diagnostic` wrapper so we can attach a
    /// `NamedSource` without pulling in a new `thiserror` dependency
    /// in the binary crate.
    struct WrappedPipelineError {
        filename: String,
        message: String,
        src: Option<NamedSource<String>>,
    }

    impl fmt::Debug for WrappedPipelineError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}: {}", self.filename, self.message)
        }
    }

    impl fmt::Display for WrappedPipelineError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "pipeline error in {}: {}", self.filename, self.message)
        }
    }

    impl std::error::Error for WrappedPipelineError {}

    impl Diagnostic for WrappedPipelineError {
        fn code<'a>(&'a self) -> Option<Box<dyn fmt::Display + 'a>> {
            Some(Box::new("clinker::pipeline_error"))
        }
        fn severity(&self) -> Option<Severity> {
            Some(Severity::Error)
        }
        fn source_code(&self) -> Option<&dyn miette::SourceCode> {
            self.src.as_ref().map(|s| s as &dyn miette::SourceCode)
        }
    }

    let wrapped = WrappedPipelineError {
        filename: filename.clone(),
        message: err.to_string(),
        src: source_text
            .as_ref()
            .map(|s| NamedSource::new(filename.clone(), s.clone())),
    };

    let report = Report::new(wrapped);
    eprintln!("{report:?}");
}

/// Print every diagnostic from a channel-overlay result and convert
/// any error-severity entry into a `PipelineError::Compilation` so
/// the run aborts before executor init.
fn abort_on_overlay_errors(
    overlay: &clinker_channel::ChannelOverlayResult,
) -> Result<(), PipelineError> {
    use clinker_core_types::Severity;
    let mut had_error = false;
    let mut messages: Vec<String> = Vec::new();
    for d in &overlay.diagnostics {
        let severity_label = match d.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
            Severity::Note => "note",
        };
        eprintln!("{}: [{}] {}", severity_label, d.code, d.message);
        if matches!(d.severity, Severity::Error) {
            had_error = true;
            messages.push(format!("[{}] {}", d.code, d.message));
        }
    }
    if had_error {
        return Err(PipelineError::Compilation {
            transform_name: String::from("<channel overlay>"),
            messages,
        });
    }
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

fn run(args: &RunArgs) -> Result<u8, PipelineError> {
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
    let channel_layout = clinker_toml.channel.clone();
    let group_layout = clinker_toml.group.clone();
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
    let target_name = target_stem_of(&args.config.to_string_lossy());
    let overlay_resolution = if args.channel.is_none() && args.groups.is_empty() {
        None
    } else {
        Some(
            clinker_channel::resolve(
                &workspace_root,
                &channel_layout,
                &group_layout,
                &target_name,
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
    // array_paths / options) to the parsed config before it is validated and
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

    let mut compile_ctx =
        clinker_plan::config::CompileContext::with_pipeline_dir(workspace_root, pipeline_dir);
    compile_ctx.allow_absolute_paths = args.allow_absolute_paths;
    if let Some(res) = &overlay_resolution {
        compile_ctx.overlay_ops = res.op_stream().to_vec();
        // Resolve the winning `config:` value per composition node so the
        // compile-time fold substitutes it into `$config.<param>` reads. The
        // ProvenanceDb still records the full layer chain (post-compile
        // overlay), so this drives execution without double-applying to it.
        compile_ctx.config_overrides = res.effective_config_overrides();
    }

    // Run identity values flow through Output path templates and the
    // provenance sidecar. Generated before --explain so resolved-path
    // summaries match the values the actual run would use. The id pair
    // re-rolls per invocation; consumers correlate runs via batch_id.
    let execution_id = uuid::Uuid::now_v7().to_string();
    let batch_id = args.resolved_batch_id();
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
    // text explain and the config-validation dry run want it; the JSON and
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
        let mut compiled_plan =
            pipeline_config
                .compile(&compile_ctx)
                .map_err(|diags| PipelineError::Compilation {
                    transform_name: String::new(),
                    messages: diags.iter().map(|d| d.message.clone()).collect(),
                })?;
        if let Some(res) = &overlay_resolution {
            let overlay = res.apply_config_and_vars(&mut compiled_plan, &pipeline_config);
            abort_on_overlay_errors(&overlay)?;
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
        let mut compiled_plan =
            cfg.compile(&compile_ctx)
                .map_err(|diags| PipelineError::Compilation {
                    transform_name: String::new(),
                    messages: diags.iter().map(|d| d.message.clone()).collect(),
                })?;
        if let Some(res) = &overlay_resolution {
            let overlay = res.apply_config_and_vars(&mut compiled_plan, cfg);
            abort_on_overlay_errors(&overlay)?;
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

    if args.dry_run && args.dry_run_n.is_none() {
        // Config-validation-only mode (no -n)
        tracing::info!(
            "Dry run: config valid, {} inputs, {} outputs, {} transforms",
            pipeline_config.source_configs().count(),
            pipeline_config.output_configs().count(),
            pipeline_config.transform_node_count(),
        );
        return Ok(0);
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

    // Open readers for ALL sources (primary + lookup references) and
    // writers for ALL outputs. The first source's identity seeds the DLQ
    // sidecar's global `_cxl_dlq_source_file` fallback: a file source uses
    // its path, a pathless network source uses the same `<source:NAME>`
    // synthetic id the executor stamps per record.
    let first_source = pipeline_config
        .source_configs()
        .next()
        .expect("pipeline has at least one source");
    let input_path = if first_source.transport.is_file() {
        first_source.path_str().to_string()
    } else {
        format!("<source:{}>", first_source.name)
    };

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
    let mut compiled_plan =
        pipeline_config
            .compile(&compile_ctx)
            .map_err(|diags| PipelineError::Compilation {
                transform_name: String::new(),
                messages: diags.iter().map(|d| d.message.clone()).collect(),
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

    // Outputs are written atomically: each output writes to a sibling
    // tempfile, then renames into place after the pipeline completes
    // successfully. On crash or pipeline error, the writing tempfile is
    // left in place (and its path logged) so an operator can inspect
    // partial output without the final path showing a truncated file.
    //
    // Cross-process race-safety for `if_exists: unique_suffix` comes
    // from `OpenOptions::create_new` reservations: each output's
    // resolved path holds a 0-byte placeholder file from the moment
    // `open_output` returns until persist atomically replaces it.  The
    // placeholder is wrapped in a `tempfile::TempPath`, mirroring what
    // the rest of the codebase already does for tempfile cleanup, so
    // any unwind path (panic, mid-persist failure, the explicit Err
    // arm below) auto-unlinks remaining placeholders via `Drop`.
    let mut writers: std::collections::HashMap<String, Box<dyn std::io::Write + Send>> =
        std::collections::HashMap::new();
    let mut output_temps: Vec<PendingOutput> = Vec::new();
    // output_name → final resolved path, kept after output_temps is
    // consumed by the persist loop so the provenance sidecar can
    // record the actual file written (not the bare template-rendered
    // path on OutputConfig).
    let mut resolved_output_paths: std::collections::HashMap<String, std::path::PathBuf> =
        std::collections::HashMap::new();
    let mut fan_out_writers: std::collections::HashMap<
        String,
        std::collections::HashMap<std::sync::Arc<str>, Box<dyn std::io::Write + Send>>,
    > = std::collections::HashMap::new();
    for output in pipeline_config.output_configs() {
        // Split outputs route file creation through SplittingWriter's
        // per-`{seq}` factory inside build_format_writer, which applies
        // `if_exists` itself. The atomic tempfile pattern below is for
        // single-file outputs only; for splits, install a sink writer
        // here so the executor's drop of `raw_writer` is harmless.
        if output.split.is_some() {
            writers.insert(output.name.clone(), Box::new(std::io::sink()));
            resolved_output_paths.insert(output.name.clone(), output.path.clone().into());
            continue;
        }
        // Fan-out path: when the plan flagged this Output for per-
        // source-file routing, render the template once per matched
        // source file. Each rendered path gets its own writer; the
        // dispatcher routes records by `$source.file` Arc.
        if output_is_fan_out(compiled_plan.dag(), &output.name) {
            let upstream_source = upstream_source_for_output(compiled_plan.dag(), &output.name);
            let files = upstream_source
                .as_ref()
                .and_then(|s| source_files_by_name.get(s.as_str()))
                .cloned()
                .unwrap_or_default();
            let mut per_file: std::collections::HashMap<
                std::sync::Arc<str>,
                Box<dyn std::io::Write + Send>,
            > = std::collections::HashMap::new();
            for path in files {
                let file_arc: std::sync::Arc<str> =
                    std::sync::Arc::from(path.to_string_lossy().into_owned());
                let label = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("source")
                    .to_string();
                let resolved = output
                    .path
                    .replace("{source_file}", &label)
                    .replace("{source_path}", &path.to_string_lossy());
                let resolved_path = std::path::PathBuf::from(&resolved);
                if let Some(parent) = resolved_path.parent()
                    && !parent.as_os_str().is_empty()
                    && !parent.exists()
                {
                    std::fs::create_dir_all(parent)?;
                }
                let file = std::fs::File::create(&resolved_path)?;
                per_file.insert(file_arc, Box::new(file));
            }
            fan_out_writers.insert(output.name.clone(), per_file);
            // Skip the atomic-tempfile path; fan-out outputs write
            // directly. Atomic per-file commit is a follow-up.
            continue;
        }
        let bare = std::path::PathBuf::from(&output.path);
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
        let (final_path, reservation_file) =
            clinker_exec::output::open::open_output(output.if_exists, args.force, path_for_n)?;
        drop(reservation_file);
        let reservation = tempfile::TempPath::try_from_path(&final_path)?;
        let parent = final_path.parent().filter(|p| !p.as_os_str().is_empty());
        let temp = match parent {
            Some(dir) => {
                if !dir.exists() {
                    std::fs::create_dir_all(dir)?;
                }
                tempfile::NamedTempFile::new_in(dir)?
            }
            None => tempfile::NamedTempFile::new_in(".")?,
        };
        let handle = temp.reopen()?;
        let writer: Box<dyn std::io::Write + Send> = Box::new(handle);
        writers.insert(output.name.clone(), writer);
        resolved_output_paths.insert(output.name.clone(), final_path.clone());
        output_temps.push(PendingOutput {
            name: output.name.clone(),
            final_path,
            temp,
            reservation,
        });
    }

    let registry = clinker_exec::executor::WriterRegistry {
        single: writers,
        fan_out: fan_out_writers,
    };
    // Fresh per-run shutdown token. `ShutdownToken::new()` auto-registers
    // with the process-wide signal-handler registry installed in `main`,
    // so a SIGINT/SIGTERM during the run trips it; the executor polls it
    // at operator chunk boundaries and unwinds gracefully.
    let shutdown_token = clinker_exec::pipeline::shutdown::ShutdownToken::new();
    let run_params = clinker_exec::executor::PipelineRunParams {
        execution_id: execution_id.clone(),
        batch_id: batch_id.clone(),
        pipeline_vars: channel_pipeline_vars,
        static_vars: channel_static_vars,
        source_vars: channel_source_vars,
        record_vars: channel_record_vars,
        shutdown_token: Some(shutdown_token),
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
    let report = match PipelineExecutor::run_plan_with_readers_writers_in_context(
        &compiled_plan,
        readers,
        registry,
        &run_params,
        compile_ctx.without_overlay_ops(),
    ) {
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
            // Reservations auto-unlink via TempPath::Drop when output_temps
            // is dropped at end of scope. We just need to preserve the
            // writing tempfiles for operator inspection and log.
            for pending in output_temps {
                let kept = pending.temp.into_temp_path().keep().ok();
                tracing::warn!(
                    output = %pending.name,
                    final_path = %pending.final_path.display(),
                    partial_path = ?kept,
                    "pipeline failed; partial output preserved at temp path",
                );
                // pending.reservation drops here, unlinking the placeholder.
            }
            return Err(e);
        }
    };

    // Pipeline succeeded — atomically promote each tempfile to its
    // final path. Order: persist first (rename atomically replaces the
    // reservation placeholder); only on success forget the reservation
    // so its TempPath::Drop does not unlink the now-persisted output.
    // Then fsync the parent dir so the rename's metadata is durable
    // across a crash (Linux ext4/xfs default mount options do not
    // synchronously flush parent-dir entries on rename).
    for pending in output_temps {
        let PendingOutput {
            name: _,
            final_path,
            temp,
            reservation,
        } = pending;
        temp.persist(&final_path).map_err(|e| {
            tracing::error!(
                final_path = %final_path.display(),
                "failed to atomically rename output into place; temp file preserved",
            );
            std::io::Error::other(format!(
                "atomic output rename failed for {}: {}",
                final_path.display(),
                e.error
            ))
        })?;
        // Persist replaced the placeholder atomically; the file at
        // `final_path` is now the actual output. Forget the TempPath
        // so Drop does not unlink it. Equivalent to `keep()` but avoids
        // surfacing platform-specific keep failures that, post-persist,
        // would just confuse the operator.
        std::mem::forget(reservation);
        if let Some(parent) = final_path.parent().filter(|p| !p.as_os_str().is_empty())
            && let Err(e) = fsync_dir(parent)
        {
            tracing::warn!(
                final_path = %final_path.display(),
                error = %e,
                "fsync(parent_dir) failed; rename metadata may not survive a crash",
            );
        }
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
    if !dlq_entries.is_empty()
        && let Some(ref dlq_config) = pipeline_config.error_handling.dlq
    {
        let buckets = clinker_exec::dlq::partition_dlq_entries(dlq_entries, dlq_config);
        if !buckets.is_empty() {
            // DLQ user-row columns come from the source's authored
            // `schema:` declaration, not from re-reading the input file.
            // The authored schema is the runtime schema for every
            // transport, and a pathless network source has no file to
            // re-open, so deriving columns from the declaration is the
            // single correct path for both file and network sources.
            let input_schema = {
                let mut builder = clinker_record::SchemaBuilder::new();
                if let Some(body) = pipeline_config.source_bodies().next() {
                    for col in body.schema.bound_columns().unwrap_or_default() {
                        builder = builder.with_field(col.name.as_str());
                    }
                }
                builder.build()
            };
            let include_reason = dlq_config.include_reason.unwrap_or(true);
            let include_source_row = dlq_config.include_source_row.unwrap_or(true);
            for (target_path, bucket_entries) in &buckets {
                if bucket_entries.is_empty() {
                    continue;
                }
                let target_dir = target_path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| std::path::PathBuf::from("."));
                if !target_dir.exists() {
                    std::fs::create_dir_all(&target_dir)?;
                }
                let dlq_temp = tempfile::NamedTempFile::new_in(&target_dir)?;
                let dlq_temp_handle = dlq_temp.reopen()?;
                let owned: Vec<clinker_exec::executor::DlqEntry> =
                    bucket_entries.iter().map(|e| (*e).clone()).collect();
                clinker_exec::dlq::write_dlq(
                    dlq_temp_handle,
                    &owned,
                    &input_schema,
                    &input_path,
                    include_reason,
                    include_source_row,
                )
                .map_err(PipelineError::Format)?;
                dlq_temp.persist(target_path).map_err(|e| {
                    tracing::error!(
                        final_path = %target_path.display(),
                        "failed to atomically rename DLQ output into place; temp file preserved",
                    );
                    PipelineError::Io(std::io::Error::other(format!(
                        "atomic DLQ rename failed for {}: {}",
                        target_path.display(),
                        e.error
                    )))
                })?;
            }
        }
    }

    // Provenance sidecars for outputs that opted in via `write_meta`.
    // Per-output record/byte/route counts are not yet surfaced from the
    // executor; identity, timing, and DLQ-by-category come through.
    for output in pipeline_config.output_configs() {
        if !output.write_meta {
            continue;
        }
        let mut dlq_counts: std::collections::BTreeMap<String, u64> =
            std::collections::BTreeMap::new();
        for e in dlq_entries {
            if e.stage.as_deref() == Some(&format!("output:{}", output.name)) {
                *dlq_counts.entry(format!("{:?}", e.category)).or_default() += 1;
            }
        }
        let elapsed_ms = (report.finished_at - report.started_at)
            .num_milliseconds()
            .max(0) as u64;
        let hash_full = clinker_exec::output::sidecar::hash_to_hex(&pipeline_hash);
        let hash_short = hash_full[..8.min(hash_full.len())].to_string();
        let target = resolved_output_paths
            .get(&output.name)
            .cloned()
            .unwrap_or_else(|| std::path::PathBuf::from(&output.path));
        let sidecar = clinker_exec::output::sidecar::OutputSidecar {
            pipeline_path: args.config.to_string_lossy().into_owned(),
            pipeline_hash: hash_full,
            pipeline_hash_short: hash_short,
            channel: None,
            clinker_version: env!("CARGO_PKG_VERSION").to_string(),
            run_started_at: report.started_at.to_rfc3339(),
            run_finished_at: report.finished_at.to_rfc3339(),
            elapsed_total_ms: elapsed_ms,
            execution_id: Some(execution_id.clone()),
            batch_id: Some(batch_id.clone()),
            output_name: output.name.clone(),
            resolved_path: target.to_string_lossy().into_owned(),
            record_count: 0,
            bytes_written: 0,
            dlq_counts,
            route_counts: std::collections::BTreeMap::new(),
            node_timings_ms: std::collections::BTreeMap::new(),
        };
        if let Err(e) = clinker_exec::output::sidecar::write_sidecar(&target, &sidecar) {
            tracing::warn!(
                "failed to write provenance sidecar for output {:?}: {e:?}",
                output.name
            );
        }
    }

    tracing::info!(
        "Pipeline complete: {} total, {} ok, {} written, {} dlq",
        counters.total_count,
        counters.ok_count,
        counters.records_written,
        counters.dlq_count
    );

    // Per-stage actual spill volume at end-of-run, so an operator can compare
    // each stage's real spilled bytes against the pre-run `--explain` per-stage
    // estimate (the calibration loop #176 exists for). Printed only when a stage
    // actually spilled; a run that stayed in memory adds no noise.
    if !report.per_stage_spill_bytes.is_empty() {
        println!("=== Spill Volume (actual, per stage) ===");
        for (stage, bytes) in &report.per_stage_spill_bytes {
            println!("  {stage} → {bytes} bytes");
        }
        println!(
            "  Total: {} bytes (compare against the --explain estimate)",
            report.cumulative_spill_bytes
        );
    }

    // Exit codes per spec §10.2. An interrupted run takes precedence:
    // the pipeline drained what it could before unwinding on the
    // shutdown signal, so report the conventional SIGINT status (130)
    // even when some DLQ entries also landed.
    let exit_code: u8 = if report.interrupted {
        130
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
            error: None,
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

    Ok(exit_code)
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
    loop {
        match &dag.graph[cur] {
            PlanNode::Source { name, .. } => return Some(name.clone()),
            PlanNode::Merge { .. } => return None,
            PlanNode::Combine { .. } => {
                // Pick the FilePartitioned parent (the driver after
                // the partition propagation pass). Falls back to
                // `None` if the combine destroyed partitioning.
                let parents: Vec<_> = dag.graph.neighbors(cur).collect();
                let next = parents.into_iter().find(|p| {
                    dag.node_properties.get(p).is_some_and(|np| {
                        matches!(
                            np.partitioning.kind,
                            PartitioningKind::FilePartitioned { .. }
                        )
                    })
                })?;
                cur = next;
            }
            _ => {
                cur = dag.graph.neighbors(cur).next()?;
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

/// One per-output bookkeeping record carried from writer-loop to
/// persist-loop. The `reservation` field is the 0-byte placeholder
/// created by `open_output`; its `TempPath::Drop` auto-unlinks if
/// anything between writer-loop and persist tears down (panic, Err
/// arm, mid-persist failure).
struct PendingOutput {
    name: String,
    final_path: std::path::PathBuf,
    temp: tempfile::NamedTempFile,
    reservation: tempfile::TempPath,
}

/// Force durable persistence of `dir`'s entry metadata to disk.
///
/// Called immediately after `tempfile::persist` (rename) so a crash
/// between the rename returning and the kernel writing the parent-dir
/// metadata cannot leave the rename invisible. Linux ext4/xfs default
/// mount options do not implicitly fsync the parent dir on rename;
/// see <https://yakking.branchable.com/posts/atomic-file-creation-tmpfile/>.
///
/// On non-Unix targets this is a no-op — opening directories for
/// `fsync` is a Unix-ism, and Windows' `MoveFileExW` provides
/// equivalent durability via the journal.
#[cfg(unix)]
fn fsync_dir(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn fsync_dir(_path: &std::path::Path) -> std::io::Result<()> {
    Ok(())
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

fn run_explain(args: &ExplainArgs) -> Result<(), Box<dyn std::error::Error>> {
    // Mode 1: --code — look up error/warning code documentation.
    if let Some(ref code) = args.code {
        match clinker_plan::plan::explain_provenance::explain_code(code) {
            Some(doc) => {
                print!("{doc}");
                return Ok(());
            }
            None => {
                return Err(format!(
                    "unknown diagnostic code '{code}'. Valid codes: E101-E108, E150b-E150e, \
                     E15Y, E300/E301/E303-E313/E319, E320/E321/E323, E330-E354, \
                     W101/W302/W305/W306"
                )
                .into());
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
    // Strip the full `.comp.yaml` / `.yaml` suffix so a composition target
    // resolves to the bare stem the discovery layer expects, mirroring `run`.
    let target_name = target_stem_of(&config_path.to_string_lossy());
    let overlay_resolution = if args.channel.is_none() && args.groups.is_empty() {
        None
    } else {
        Some(
            clinker_channel::resolve(
                &workspace_root,
                &clinker_toml.channel,
                &clinker_toml.group,
                &target_name,
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
    }

    let mut compiled_plan = pipeline_config.compile(&compile_ctx).map_err(|diags| {
        let messages: Vec<String> = diags.iter().map(|d| d.message.clone()).collect();
        format!("compilation failed:\n{}", messages.join("\n"))
    })?;

    if let Some(res) = &overlay_resolution {
        use clinker_core_types::Severity;
        let overlay = res.apply_config_and_vars(&mut compiled_plan, &pipeline_config);
        let errors: Vec<String> = overlay
            .diagnostics
            .iter()
            .filter(|d| matches!(d.severity, Severity::Error))
            .map(|d| format!("[{}] {}", d.code, d.message))
            .collect();
        if !errors.is_empty() {
            return Err(format!("overlay application failed:\n{}", errors.join("\n")).into());
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
    String,
> {
    let yaml = std::fs::read_to_string(base_path)
        .map_err(|e| format!("cannot read {}: {e}", base_path.display()))?;
    let interpolated = clinker_plan::config::interpolate_env_vars(&yaml, &[])
        .map_err(|e| format!("environment variable interpolation failed: {e}"))?;
    let mut config: clinker_plan::config::PipelineConfig =
        clinker_plan::yaml::from_str(&interpolated)
            .map_err(|e| format!("YAML parse error: {e}"))?;

    // Per-source patches shape the parsed config before compile, so the
    // effective DAG this reports reflects the same schema / array_paths /
    // options changes a `run --channel` would execute.
    if let Some(patches) = res.source_patches() {
        clinker_plan::config::apply_source_patches(&mut config, patches)
            .map_err(|e| e.to_string())?;
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

    let mut plan = config.compile(&ctx).map_err(|diags| {
        let messages: Vec<String> = diags
            .iter()
            .map(|d| format!("[{}] {}", d.code, d.message))
            .collect();
        format!("compilation failed:\n{}", messages.join("\n"))
    })?;
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
    for ((node, param), resolved) in plan.provenance().iter() {
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
        rows.push(format!(
            "  {node}.{param} = {}  [{}]  (base: {})",
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
    // Strip the full `.comp.yaml` / `.yaml` suffix (not just the last
    // extension) so a composition target `score.comp.yaml` resolves to the bare
    // stem `score` the discovery layer expects — matching `channels lint`.
    let file_name = args
        .target
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or("target path has no file name")?;
    let target_name = target_stem_of(file_name);

    let res = clinker_channel::resolve(
        &workspace_root,
        &clinker_toml.channel,
        &clinker_toml.group,
        &target_name,
        args.channel.as_deref(),
        &args.groups,
        !args.no_auto_groups,
    )
    .map_err(|e| format!("overlay resolution failed: {e}"))?;

    let (config, plan, overlay) = compile_effective_plan(&args.target, &workspace_root, &res)?;

    // Overlay report (deterministic) followed by the effective DAG for context.
    print!("{}", render_resolved(&plan, &overlay, &res, &target_name));
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

    let channels = clinker_channel::scan_channels(&clinker_toml.channel, &workspace_root).map_err(
        |diags| {
            let msgs: Vec<String> = diags
                .iter()
                .map(|d| format!("[{}] {}", d.code, d.message))
                .collect();
            format!("channel scan failed:\n{}", msgs.join("\n"))
        },
    )?;

    let mut checked = 0usize;
    let mut failures: Vec<(String, String, Vec<String>)> = Vec::new();

    for channel in &channels {
        // Every overlay file in the tenant folder is a (channel × target) combo.
        for overlay_path in overlay_files_in(&channel.dir) {
            let overlay_file = match clinker_channel::OverlayFile::load(&overlay_path) {
                Ok(o) => o,
                Err(e) => {
                    failures.push((
                        channel.id.clone(),
                        overlay_path.display().to_string(),
                        vec![format!("overlay parse error: {e}")],
                    ));
                    continue;
                }
            };
            let target_name = target_stem_of(&overlay_file.channel.target);
            let base_path = channel.dir.join(&overlay_file.channel.target);

            checked += 1;
            let res = match clinker_channel::resolve(
                &workspace_root,
                &clinker_toml.channel,
                &clinker_toml.group,
                &target_name,
                Some(&channel.id),
                &[],
                true,
            ) {
                Ok(r) => r,
                Err(e) => {
                    failures.push((
                        channel.id.clone(),
                        target_name.clone(),
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
                        failures.push((channel.id.clone(), target_name.clone(), errs));
                    }
                }
                Err(msg) => {
                    failures.push((
                        channel.id.clone(),
                        target_name.clone(),
                        msg.lines().map(str::to_string).collect(),
                    ));
                }
            }
        }
    }

    if failures.is_empty() {
        println!(
            "channels lint: OK — {checked} (target × overlay) combination(s) across {} channel(s) compiled clean",
            channels.len()
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

/// The candidate overlay files in a tenant folder: every `*.yaml` except the
/// `channel.cfg.yaml` manifest. Non-recursive, symlinks skipped.
fn overlay_files_in(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return files;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name == clinker_channel::CHANNEL_MANIFEST_FILE {
            continue;
        }
        if name.ends_with(".yaml") {
            files.push(path);
        }
    }
    files.sort();
    files
}

/// The bare target stem of a `channel.target:` path (strip `.comp.yaml` /
/// `.yaml` and the directory).
fn target_stem_of(target: &str) -> String {
    let file = std::path::Path::new(target)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(target);
    file.strip_suffix(".comp.yaml")
        .or_else(|| file.strip_suffix(".yaml"))
        .unwrap_or(file)
        .to_string()
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
                    LabelOutcome::Created => "created manifest",
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
enum LabelOutcome {
    /// The manifest did not exist and was created with the label.
    Created,
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
        let block = render_labels_block(&[(key.to_string(), value.clone())]);
        let text = format!("channel:\n  name: {channel_id}\n{}\n", block.join("\n"));
        std::fs::write(manifest_path, text)
            .map_err(|e| format!("writing {}: {e}", manifest_path.display()))?;
        return Ok(LabelOutcome::Created);
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
    use clap::Parser;

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
            "channel:\n  name: acme\n# keep me\nlabels:\n  region: west\nconfig:\n  fraud.threshold: 0.9\n",
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
            text.contains("fraud.threshold: 0.9"),
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
            "channel:\n  name: acme\nlabels:\n  region: west\n# note\n  region2: east\nconfig:\n  k.v: 1\n",
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
    fn label_set_creates_absent_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let dir = root.join("channel").join("globex");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(clinker_channel::CHANNEL_MANIFEST_FILE);

        let out = set_manifest_label(
            &path,
            "globex",
            "tier",
            &serde_json::Value::from("enterprise"),
        )
        .unwrap();
        assert!(matches!(out, LabelOutcome::Created));
        let m = clinker_channel::ChannelManifest::load(&path).unwrap();
        assert_eq!(m.channel.name, "globex");
        assert_eq!(
            m.labels.get("tier"),
            Some(&serde_json::Value::from("enterprise"))
        );
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
            "group:\n  name: enterprise\n  match: 'tier == \"enterprise\"'\n",
        )
        .unwrap();
        write_channel_manifest(
            root,
            "acme",
            "channel:\n  name: acme\nlabels:\n  tier: enterprise\n",
        );
        write_channel_manifest(
            root,
            "beta",
            "channel:\n  name: beta\nlabels:\n  tier: basic\n",
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
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes().ok(), Some(524288)),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_suffix_m() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--memory-limit", "256M", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes().ok(), Some(268435456)),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_suffix_g() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--memory-limit", "2G", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes().ok(), Some(2147483648)),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_bare_bytes() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--memory-limit", "1000000", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes().ok(), Some(1000000)),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_default_memory_limit() {
        let cli = Cli::try_parse_from(["clinker", "run", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert_eq!(args.memory_limit_bytes().ok(), Some(512 * 1024 * 1024))
            }
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
