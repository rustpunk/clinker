use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};

use clinker_exec::executor::PipelineExecutor;
use clinker_exec::metrics::{self, ExecutionMetrics};
use clinker_plan::config::utils::parse_memory_limit_bytes;
use clinker_plan::error::PipelineError;

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
Use --field to trace where a composition config value comes from across all \
configuration layers (composition defaults, channel defaults, channel fixed). \
Use --code to look up the documentation for a diagnostic code (composition codes \
E101–E108, combine codes E300-E319 and W302/W305/W306, and W101).",
        after_long_help = "\
EXAMPLES:
  # Show provenance for a composition config field
  clinker explain pipeline.yaml --field enrich1.fuzzy_threshold

  # Show provenance with a channel overlay applied
  clinker explain pipeline.yaml --field enrich1.fuzzy_threshold --channel acme_prod.channel.yaml

  # Look up error code documentation
  clinker explain --code E105"
    )]
    Explain(ExplainArgs),
}

/// Output format for --explain.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ExplainFormat {
    /// Human-readable ASCII text with branch/merge indicators.
    Text,
    /// Structured JSON for Kiln canvas consumption.
    Json,
    /// Graphviz DOT for static visualization.
    Dot,
}

/// Arguments for `clinker run`.
#[derive(Parser, Debug)]
pub struct RunArgs {
    /// Path to the pipeline YAML configuration file
    pub config: PathBuf,

    /// Memory budget (supports K/M/G suffixes), default 256M
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

    /// Channel YAML file to overlay before execution.
    /// The channel can override or add `$vars.*` / `$pipeline.*` /
    /// `$source.*` / `$record.*` defaults. Reserved system field names
    /// are rejected.
    #[arg(long, help_heading = "Configuration")]
    pub channel: Option<PathBuf>,
}

impl RunArgs {
    /// Resolve memory limit from CLI flag or default (256MB).
    pub fn memory_limit_bytes(&self) -> u64 {
        parse_memory_limit_bytes(self.mem_limit.as_deref().or(Some("256M")))
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

    /// Channel YAML file to apply before provenance lookup
    #[arg(long)]
    pub channel: Option<PathBuf>,

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
                        | PipelineError::CombineMissingMatch { .. } => ExitCode::from(1),
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

fn run(args: &RunArgs) -> Result<u8, PipelineError> {
    // Resolve CLINKER_ENV
    if let Some(env_name) = args
        .env
        .clone()
        .or_else(|| std::env::var("CLINKER_ENV").ok())
    {
        unsafe { std::env::set_var("CLINKER_ENV", env_name) };
    }

    let mut pipeline_config = clinker_plan::config::load_config_with_vars(&args.config, &[])
        .map_err(PipelineError::Config)?;

    // Load the channel binding once if `--channel` was supplied. The
    // overlay itself runs against each `compile()` result below; the
    // binding is borrowed by the `--explain` arm and the main run.
    let channel_binding = match args.channel.as_deref() {
        Some(path) => Some(clinker_channel::ChannelBinding::load(path).map_err(|e| {
            PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
                "channel '{}': {e}",
                path.display(),
            )))
        })?),
        None => None,
    };
    if let Some(b) = &channel_binding {
        let target = match &b.target {
            clinker_channel::ChannelTarget::Pipeline(p) => p,
            clinker_channel::ChannelTarget::Composition(p) => p,
        };
        let canon_target = std::fs::canonicalize(target).ok();
        let canon_config = std::fs::canonicalize(&args.config).ok();
        if let (Some(t), Some(c)) = (canon_target.as_ref(), canon_config.as_ref())
            && t != c
        {
            eprintln!(
                "W104: channel {:?} targets {:?} but run loaded {:?}; proceeding",
                b.name,
                target.display(),
                args.config.display(),
            );
        }
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
    // Loaded and validated here, before compile and before any reader or
    // writer is opened, so a misconfigured spill volume fails the run at
    // startup with a clear message rather than at the first spill — the trap
    // DuckDB fell into when its temp-directory setting was honored only
    // lazily (duckdb/duckdb#9401). The validated directory threads into the
    // run via `PipelineRunParams.spill_root_dir`; absent config keeps the
    // historical OS-temp-dir default.
    let storage_config = clinker_plan::config::ClinkerToml::load_from_workspace(&workspace_root)
        .map_err(storage_config_error)?
        .storage;
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

    let mut compile_ctx =
        clinker_plan::config::CompileContext::with_pipeline_dir(workspace_root, pipeline_dir);
    compile_ctx.allow_absolute_paths = args.allow_absolute_paths;

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
        channel: channel_binding.as_ref().map(|b| b.name.as_str()),
        pipeline_hash,
        timestamp: Some(&timestamp_str),
        execution_id: Some(&execution_id),
        batch_id: Some(&batch_id),
        n: None,
        unique_suffix_width: 0,
    };
    clinker_plan::config::path_template::resolve_output_path_templates_in_place(
        &mut pipeline_config,
        &template_ctx,
    )
    .map_err(PipelineError::Config)?;

    if args.explain.is_some() || (args.dry_run && args.dry_run_n.is_none()) {
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
        if let Some(binding) = &channel_binding {
            let overlay = clinker_channel::apply_channel_overlay(
                &mut compiled_plan,
                binding,
                &pipeline_config,
            );
            abort_on_overlay_errors(&overlay)?;
        }
        let dag = compiled_plan.dag();
        let artifacts = compiled_plan.artifacts();
        match format {
            ExplainFormat::Text => {
                print!(
                    "{}",
                    dag.explain_text_with_artifacts(&pipeline_config, artifacts)
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
            }
            ExplainFormat::Json => {
                let view = clinker_plan::plan::execution::ExplainJson::new(dag, artifacts);
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

    // Channel-resolved var overrides land here when --channel is set.
    // Populated below from `apply_channel_overlay` after compile; the
    // executor layers them atop Transform-declared defaults at init.
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
    for body in pipeline_config.source_bodies() {
        let source = &body.source;
        match &source.transport {
            clinker_plan::config::SourceTransport::File => {
                let outcome = clinker_exec::source::discovery::discover(source, &workspace_root)
                    .map_err(|e| {
                        use clinker_exec::source::discovery::DiscoveryError;
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
                let mut slots: Vec<clinker_exec::source::multi_file::FileSlot> = Vec::new();
                let mut paths: Vec<std::path::PathBuf> = Vec::new();
                for f in outcome.files() {
                    let file = std::fs::File::open(&f.path)?;
                    slots.push(clinker_exec::source::multi_file::FileSlot::new(
                        f.path.clone(),
                        Box::new(file),
                    ));
                    paths.push(f.path.clone());
                }
                source_files_by_name.insert(source.name.clone(), paths);
                // EmptyWarn / EmptySkip outcomes leave `slots` empty; the
                // executor short-circuits via the empty-list guard upstream.
                if slots.is_empty() {
                    // Stash a single empty reader so the executor's "missing
                    // reader" check passes. Records flow through as zero-row
                    // sources.
                    slots.push(clinker_exec::source::multi_file::FileSlot::new(
                        "<empty>",
                        Box::new(std::io::empty()),
                    ));
                }
                readers.insert(
                    source.name.clone(),
                    clinker_exec::executor::SourceInput::Files(slots),
                );
            }
            clinker_plan::config::SourceTransport::Rest(rest_cfg) => {
                // The rest transport bypasses fs discovery entirely. The
                // reader is a row yielder driven on the ingest thread; the
                // `{source_file}` fan-out side-table gets no file paths, so
                // the `<source:NAME>` synthetic id is the stable identity.
                let reader = clinker_net::build_rest_source(
                    rest_cfg.clone(),
                    source,
                    &body.schema.columns,
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
    // Compile the plan up front so output-side fan-out detection
    // (§5) can read `fan_out_per_source_file` flags before the writer
    // setup decides whether to open one writer or N.
    let mut compiled_plan = pipeline_config.compile(&compile_ctx).expect("compile");
    if let Some(binding) = &channel_binding {
        let overlay =
            clinker_channel::apply_channel_overlay(&mut compiled_plan, binding, &pipeline_config);
        abort_on_overlay_errors(&overlay)?;
        channel_static_vars = overlay.static_vars;
        channel_pipeline_vars = overlay.pipeline_vars;
        channel_source_vars = overlay.source_vars;
        channel_record_vars = overlay.record_vars;
    }
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
    let report = match PipelineExecutor::run_plan_with_readers_writers_in_context(
        &compiled_plan,
        readers,
        registry,
        &run_params,
        compile_ctx.clone(),
    ) {
        Ok(report) => report,
        Err(e) => {
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
                    for col in &body.schema.columns {
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
                     E15Y, E300/E301/E303-E311/E313/E319, W101/W302/W305/W306"
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
    let pipeline_config: clinker_plan::config::PipelineConfig =
        clinker_plan::yaml::from_str(&interpolated)
            .map_err(|e| format!("YAML parse error: {e}"))?;

    // Resolve workspace root and pipeline_dir so composition `use:` paths
    // resolve correctly. The workspace root is the base_dir (default: CWD),
    // and pipeline_dir is the config file's parent relative to workspace_root.
    let workspace_root = args.base_dir.canonicalize()?;
    let config_parent = config_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .canonicalize()?;
    let pipeline_dir = config_parent
        .strip_prefix(&workspace_root)
        .unwrap_or_else(|_| std::path::Path::new(""))
        .to_path_buf();
    let compile_ctx =
        clinker_plan::config::CompileContext::with_pipeline_dir(&workspace_root, pipeline_dir);

    let compiled_plan = pipeline_config.compile(&compile_ctx).map_err(|diags| {
        let messages: Vec<String> = diags.iter().map(|d| d.message.clone()).collect();
        format!("compilation failed:\n{}", messages.join("\n"))
    })?;

    let output =
        clinker_plan::plan::explain_provenance::explain_field_provenance(&compiled_plan, field)
            .map_err(|e| format!("{e}"))?;

    print!("{output}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

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
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes(), 524288),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_suffix_m() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--memory-limit", "256M", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes(), 268435456),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_suffix_g() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--memory-limit", "2G", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes(), 2147483648),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_bare_bytes() {
        let cli =
            Cli::try_parse_from(["clinker", "run", "--memory-limit", "1000000", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes(), 1000000),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_default_memory_limit() {
        let cli = Cli::try_parse_from(["clinker", "run", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes(), 256 * 1024 * 1024),
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
}
