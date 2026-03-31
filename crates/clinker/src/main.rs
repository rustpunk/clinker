use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use clinker_channel::channel_override::{resolve_channel, resolve_channel_with_inheritance, ChannelOverride};
use clinker_channel::composition::{resolve_compositions, ProvenanceMap};
use clinker_channel::manifest::ChannelManifest;
use clinker_channel::workspace::WorkspaceRoot;
use clinker_core::config::load_config_with_vars;
use clinker_core::error::PipelineError;
use clinker_core::executor::PipelineExecutor;
use clinker_core::metrics::{self, ExecutionMetrics};
use clinker_core::pipeline::memory::parse_memory_limit_bytes;
use clinker_format::FormatReader;

/// CXL streaming ETL engine.
#[derive(Parser, Debug)]
#[command(name = "clinker", about = "CXL streaming ETL engine")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run a pipeline from a YAML config file
    Run(RunArgs),
    /// Metrics utilities
    Metrics {
        #[command(subcommand)]
        subcommand: MetricsCommands,
    },
}

/// Arguments for `clinker run`.
#[derive(Parser, Debug)]
pub struct RunArgs {
    /// Path to the pipeline YAML configuration file
    pub config: PathBuf,

    /// Memory budget (supports K/M/G suffixes), default 256M
    #[arg(long)]
    pub memory_limit: Option<String>,

    /// Thread pool size, default num_cpus
    #[arg(long)]
    pub threads: Option<usize>,

    /// Max DLQ records before abort, 0 = unlimited
    #[arg(long, default_value = "0")]
    pub error_threshold: u64,

    /// Pipeline batch_id, default generated UUID v7
    #[arg(long)]
    pub batch_id: Option<String>,

    /// CXL module search path
    #[arg(long, default_value = "./rules/")]
    pub rules_path: PathBuf,

    /// Print execution plan and exit (no data read)
    #[arg(long)]
    pub explain: bool,

    /// Validate config and CXL without processing data
    #[arg(long)]
    pub dry_run: bool,

    /// Suppress stderr progress output
    #[arg(long)]
    pub quiet: bool,

    /// Allow output file overwrite
    #[arg(long)]
    pub force: bool,

    /// Base directory for relative path resolution
    #[arg(long)]
    pub base_dir: Option<PathBuf>,

    /// Permit absolute paths in YAML config
    #[arg(long)]
    pub allow_absolute_paths: bool,

    /// Log level: error, warn, info, debug, trace
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Directory to spool per-execution JSON metrics files.
    /// Overrides CLINKER_METRICS_SPOOL_DIR env var and pipeline.metrics.spool_dir in YAML.
    #[arg(long)]
    pub metrics_spool_dir: Option<PathBuf>,

    /// Channel override to apply (e.g., "acme-corp")
    #[arg(long)]
    pub channel: Option<String>,

    /// Explicit path(s) to .channel.yaml override file(s). Bypasses derived-name lookup.
    /// May be specified multiple times; applied in declaration order (later wins on conflict).
    #[arg(long)]
    pub channel_path: Vec<PathBuf>,

    /// Workspace root (overrides clinker.toml auto-discovery)
    #[arg(long)]
    pub workspace: Option<PathBuf>,

    /// Active environment name for when: conditions (sets CLINKER_ENV).
    #[arg(long)]
    pub env: Option<String>,
}

impl RunArgs {
    /// Resolve memory limit from CLI flag or default (256MB).
    pub fn memory_limit_bytes(&self) -> u64 {
        parse_memory_limit_bytes(self.memory_limit.as_deref().or(Some("256M")))
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

fn main() -> ExitCode {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Run(args) => {
            let filter = args.log_level.parse::<tracing_subscriber::filter::LevelFilter>()
                .unwrap_or(tracing_subscriber::filter::LevelFilter::INFO);
            tracing_subscriber::fmt()
                .with_max_level(filter)
                .init();

            match run(args) {
                Ok(code) => ExitCode::from(code),
                Err(e) => {
                    tracing::error!("{e}");
                    match &e {
                        PipelineError::Config(_) | PipelineError::Schema(_) | PipelineError::Compilation { .. } => ExitCode::from(1),
                        PipelineError::Io(_) => ExitCode::from(4),
                        PipelineError::Eval(_) => ExitCode::from(3),
                        PipelineError::Format(_) | PipelineError::ThreadPool(_) => ExitCode::from(4),
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
    }
}

fn run(args: &RunArgs) -> Result<u8, PipelineError> {
    // 1. Workspace discovery
    let workspace = match &args.workspace {
        Some(p) => Some(WorkspaceRoot::at(p).map_err(channel_err)?),
        None => WorkspaceRoot::discover(args.config.as_path()),
    };

    // 1b. Resolve effective channel: --channel flag > default_channel from clinker.toml
    let effective_channel: Option<String> = args.channel.clone().or_else(|| {
        workspace.as_ref().and_then(|ws| {
            ws.config.default_channel.as_ref().map(|ch| {
                tracing::info!(
                    "Using default channel \"{}\" from clinker.toml (override with --channel)",
                    ch
                );
                ch.clone()
            })
        })
    });

    // 2. Resolve CLINKER_ENV from precedence chain
    let clinker_env: Option<String> = args
        .env
        .clone()
        .or_else(|| std::env::var("CLINKER_ENV").ok())
        .or_else(|| workspace.as_ref().and_then(|ws| ws.defaults.env.clone()));

    // 3. Load channel manifest → channel vars
    let mut channel_vars: Vec<(String, String)> = Vec::new();
    if let Some(ref env_name) = clinker_env {
        channel_vars.push(("CLINKER_ENV".into(), env_name.clone()));
    }
    if let (Some(id), Some(ws)) = (&effective_channel, &workspace) {
        let manifest = ChannelManifest::load(&ws.channel_dir(id)).map_err(channel_err)?;
        for (k, v) in manifest.variables {
            channel_vars.push((k, v));
        }
    }
    let vars_ref: Vec<(&str, &str)> = channel_vars
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    // 4. Load + interpolate pipeline YAML (single pass, channel vars included)
    let mut pipeline_config = load_config_with_vars(&args.config, &vars_ref)?;

    // 5. Resolve _import compositions → also get ProvenanceMap
    let mut provenance = if let Some(ws) = &workspace {
        resolve_compositions(&mut pipeline_config, ws, &vars_ref).map_err(channel_err)?
    } else {
        ProvenanceMap::new()
    };

    // 6. Apply channel override (group inheritance + channel-specific)
    if let (Some(id), Some(ws)) = (&effective_channel, &workspace) {
        if clinker_env.is_none() && !args.quiet {
            check_warn_env_unset(id, ws, &args.config, &vars_ref);
        }
        pipeline_config = resolve_channel_with_inheritance(
            pipeline_config,
            id,
            &args.config,
            ws,
            &vars_ref,
            &mut provenance,
        )
        .map_err(channel_err)?;
    }

    // 6b. Apply explicit --channel-path overrides (in declaration order; later wins on conflict)
    for path in &args.channel_path {
        if let Some(co) = ChannelOverride::load(path, &vars_ref).map_err(channel_err)? {
            let ws = workspace.as_ref().ok_or_else(|| {
                PipelineError::Config(clinker_core::config::ConfigError::Validation(
                    "--channel-path requires a workspace (clinker.toml)".into(),
                ))
            })?;
            if co.when_passes() {
                pipeline_config = resolve_channel(
                    pipeline_config,
                    &co,
                    ws,
                    &vars_ref,
                    &mut provenance,
                )
                .map_err(channel_err)?;
            }
        }
    }

    // 7. Existing logic: explain / dry_run / execute
    if args.explain {
        let plan_output = PipelineExecutor::explain(&pipeline_config)?;
        println!("{}", plan_output);
        return Ok(0);
    }

    if args.dry_run {
        tracing::info!(
            "Dry run: config valid, {} inputs, {} outputs, {} transforms",
            pipeline_config.inputs.len(),
            pipeline_config.outputs.len(),
            pipeline_config.transformations.len(),
        );
        return Ok(0);
    }

    // Resolve spool directory (CLI > env > YAML)
    let yaml_spool = pipeline_config.pipeline.metrics.as_ref()
        .and_then(|m| m.spool_dir.as_deref());
    let spool_dir = metrics::resolve_spool_dir(
        args.metrics_spool_dir.as_deref(),
        yaml_spool,
    );

    // Build runtime parameters
    let execution_id = uuid::Uuid::now_v7().to_string();
    let batch_id = args.resolved_batch_id();
    let pipeline_vars = pipeline_config.pipeline.vars.as_ref()
        .map(|v| clinker_core::config::convert_pipeline_vars(v))
        .unwrap_or_default();
    let run_params = clinker_core::executor::PipelineRunParams {
        execution_id: execution_id.clone(),
        batch_id: batch_id.clone(),
        pipeline_vars,
    };

    // Run the pipeline using file-based I/O
    let input_path = &pipeline_config.inputs[0].path;
    let output_path = &pipeline_config.outputs[0].path;

    let reader = std::fs::File::open(input_path)?;
    let writer = std::fs::File::create(output_path)?;

    let report = PipelineExecutor::run_with_readers_writers(&pipeline_config, reader, writer, &run_params)?;

    let counters = &report.counters;
    let dlq_entries = &report.dlq_entries;

    // Write DLQ if there are entries and DLQ path is configured
    if !dlq_entries.is_empty() {
        if let Some(ref dlq_config) = pipeline_config.error_handling.dlq {
            if let Some(ref dlq_path) = dlq_config.path {
                let input_schema = {
                    let f = std::fs::File::open(input_path)?;
                    let mut r = clinker_format::csv::reader::CsvReader::from_reader(
                        f,
                        clinker_format::csv::reader::CsvReaderConfig::default(),
                    );
                    r.schema().map_err(|e| PipelineError::Format(e))?
                };
                let dlq_writer = std::fs::File::create(dlq_path)?;
                let include_reason = dlq_config.include_reason.unwrap_or(true);
                let include_source_row = dlq_config.include_source_row.unwrap_or(true);
                clinker_core::dlq::write_dlq(
                    dlq_writer,
                    dlq_entries,
                    &input_schema,
                    input_path,
                    include_reason,
                    include_source_row,
                )
                .map_err(PipelineError::Format)?;
            }
        }
    }

    tracing::info!(
        "Pipeline complete: {} total, {} ok, {} dlq",
        counters.total_count, counters.ok_count, counters.dlq_count
    );

    // Exit codes per spec §10.2
    let exit_code: u8 = if counters.dlq_count > 0 { 2 } else { 0 };

    // Write execution metrics to spool directory (if configured)
    if let Some(ref dir) = spool_dir {
        let hostname = hostname_string();
        let dlq_path = pipeline_config.error_handling.dlq.as_ref()
            .and_then(|d| d.path.clone());

        let duration_ms = (report.finished_at - report.started_at).num_milliseconds();

        let execution_metrics = ExecutionMetrics {
            execution_id: execution_id.clone(),
            schema_version: 1,
            pipeline_name: pipeline_config.pipeline.name.clone(),
            config_path: args.config.to_string_lossy().into_owned(),
            hostname,
            started_at: report.started_at,
            finished_at: report.finished_at,
            duration_ms,
            exit_code,
            records_total: counters.total_count,
            records_ok: counters.ok_count,
            records_dlq: counters.dlq_count,
            execution_mode: format!("{:?}", report.execution_mode),
            peak_rss_bytes: report.peak_rss_bytes,
            thread_count: num_threads(args),
            input_files: pipeline_config.inputs.iter().map(|i| i.path.clone()).collect(),
            output_files: pipeline_config.outputs.iter().map(|o| o.path.clone()).collect(),
            dlq_path,
            error: None,
        };

        if let Err(e) = metrics::write_spool(&execution_metrics, dir) {
            tracing::warn!(
                error = %e,
                spool_dir = %dir.display(),
                execution_id = %execution_metrics.execution_id,
                pipeline_name = %execution_metrics.pipeline_name,
                records_total = execution_metrics.records_total,
                records_ok = execution_metrics.records_ok,
                records_dlq = execution_metrics.records_dlq,
                duration_ms = execution_metrics.duration_ms,
                exit_code = execution_metrics.exit_code,
                "metrics spool write failed — emitting inline"
            );
        }
    }

    Ok(exit_code)
}

fn run_metrics(cmd: &MetricsCommands) -> Result<(), std::io::Error> {
    match cmd {
        MetricsCommands::Collect(args) => {
            let entries: Vec<_> = metrics::collect_spool(&args.spool_dir)?.collect();
            let count = entries.len();

            if args.dry_run {
                println!("Would collect {count} file(s) from {}", args.spool_dir.display());
                for entry in &entries {
                    println!("  {} ({})", entry.path.display(), entry.metrics.pipeline_name);
                }
                return Ok(());
            }

            let mut written = 0usize;
            for entry in entries {
                metrics::append_ndjson(&entry.metrics, &args.output_file)?;
                if args.delete_after_collect {
                    if let Err(e) = std::fs::remove_file(&entry.path) {
                        tracing::warn!(
                            path = %entry.path.display(),
                            error = %e,
                            "metrics collect: failed to delete spool file after collection"
                        );
                    }
                }
                written += 1;
            }

            println!("Collected {written} file(s) → {}", args.output_file.display());
            Ok(())
        }
    }
}

/// Resolve thread count from CLI args or default to `num_cpus`.
fn num_threads(args: &RunArgs) -> usize {
    args.threads.unwrap_or_else(num_cpus::get)
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

/// Map ChannelError to PipelineError for uniform error handling.
fn channel_err(e: clinker_channel::error::ChannelError) -> PipelineError {
    PipelineError::Config(clinker_core::config::ConfigError::Validation(e.to_string()))
}

/// Warn if CLINKER_ENV is not set but override files have when: conditions.
fn check_warn_env_unset(
    channel_id: &str,
    workspace: &WorkspaceRoot,
    pipeline_path: &std::path::Path,
    channel_vars: &[(&str, &str)],
) {
    let channel_dir = workspace.channel_dir(channel_id);
    let derived = ChannelOverride::path_for(pipeline_path, &channel_dir);
    if let Ok(Some(co)) = ChannelOverride::load(&derived, channel_vars) {
        if co.header.when.is_some() {
            tracing::warn!(
                "CLINKER_ENV is not set; when: condition in {} will not be evaluated — override file skipped",
                derived.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

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
        let cli = Cli::try_parse_from(["clinker", "run", "--memory-limit", "512K", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes(), 524288),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_suffix_m() {
        let cli = Cli::try_parse_from(["clinker", "run", "--memory-limit", "256M", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes(), 268435456),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_suffix_g() {
        let cli = Cli::try_parse_from(["clinker", "run", "--memory-limit", "2G", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.memory_limit_bytes(), 2147483648),
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_memory_limit_bare_bytes() {
        let cli = Cli::try_parse_from(["clinker", "run", "--memory-limit", "1000000", "p.yaml"]).unwrap();
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
            "clinker", "run", "--metrics-spool-dir", "/var/spool/clinker", "p.yaml",
        ]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert_eq!(args.metrics_spool_dir, Some(PathBuf::from("/var/spool/clinker")));
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_metrics_collect_parses() {
        let cli = Cli::try_parse_from([
            "clinker", "metrics", "collect",
            "--spool-dir", "/var/spool/clinker",
            "--output-file", "/data/metrics.ndjson",
            "--delete-after-collect",
        ]).unwrap();
        match cli.command {
            Commands::Metrics { subcommand: MetricsCommands::Collect(args) } => {
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
            "clinker", "metrics", "collect",
            "--spool-dir", "/spool",
            "--output-file", "/out.ndjson",
            "--dry-run",
        ]).unwrap();
        match cli.command {
            Commands::Metrics { subcommand: MetricsCommands::Collect(args) } => {
                assert!(args.dry_run);
            }
            _ => panic!("expected Metrics::Collect command"),
        }
    }

    #[test]
    fn test_cli_run_channel_flag() {
        let cli = Cli::try_parse_from(["clinker", "run", "--channel", "acme", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.channel, Some("acme".into())),
            _ => panic!("expected Run command"),
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

    #[test]
    fn test_cli_run_channel_path_multiple() {
        let cli = Cli::try_parse_from([
            "clinker", "run", "--channel-path", "a.channel.yaml", "--channel-path", "b.channel.yaml", "p.yaml"
        ]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert_eq!(args.channel_path.len(), 2);
                assert_eq!(args.channel_path[0], PathBuf::from("a.channel.yaml"));
                assert_eq!(args.channel_path[1], PathBuf::from("b.channel.yaml"));
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_workspace_flag() {
        let cli = Cli::try_parse_from(["clinker", "run", "--workspace", "/ws", "p.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert_eq!(args.workspace, Some(PathBuf::from("/ws"))),
            _ => panic!("expected Run command"),
        }
    }
}
