use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};

use clinker_core::error::PipelineError;
use clinker_core::executor::PipelineExecutor;
use clinker_core::metrics::{self, ExecutionMetrics};
use clinker_core::pipeline::memory::parse_memory_limit_bytes;
use clinker_format::FormatReader;

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
    #[arg(long, help_heading = "Execution")]
    pub memory_limit: Option<String>,

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

fn main() -> ExitCode {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Run(args) => {
            let filter = args
                .log_level
                .parse::<tracing_subscriber::filter::LevelFilter>()
                .unwrap_or(tracing_subscriber::filter::LevelFilter::INFO);
            tracing_subscriber::fmt().with_max_level(filter).init();

            match run(args) {
                Ok(code) => ExitCode::from(code),
                Err(e) => {
                    tracing::error!("{e}");
                    match &e {
                        PipelineError::Config(_)
                        | PipelineError::Schema(_)
                        | PipelineError::Compilation { .. }
                        | PipelineError::Internal { .. }
                        | PipelineError::SortOrderViolation { .. }
                        | PipelineError::MergeSortOrderViolation { .. } => ExitCode::from(1),
                        PipelineError::Io(_) => ExitCode::from(4),
                        PipelineError::Eval(_) | PipelineError::Accumulator { .. } => {
                            ExitCode::from(3)
                        }
                        PipelineError::Format(_)
                        | PipelineError::ThreadPool(_)
                        | PipelineError::Multiple(_) => ExitCode::from(4),
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
    // Resolve CLINKER_ENV
    if let Some(env_name) = args
        .env
        .clone()
        .or_else(|| std::env::var("CLINKER_ENV").ok())
    {
        unsafe { std::env::set_var("CLINKER_ENV", env_name) };
    }

    // Load pipeline YAML directly. Composition/channel features are gone in
    // Phase 16b; Phase 16c will rebuild them.
    let yaml = std::fs::read_to_string(&args.config).map_err(PipelineError::Io)?;
    let interpolated = clinker_core::config::interpolate_env_vars(&yaml, &[]).map_err(|e| {
        PipelineError::Config(clinker_core::config::ConfigError::Validation(e.to_string()))
    })?;
    let pipeline_config: clinker_core::config::PipelineConfig =
        clinker_core::yaml::from_str(&interpolated).map_err(|e| {
            PipelineError::Config(clinker_core::config::ConfigError::Validation(e.to_string()))
        })?;

    // 7. Existing logic: explain / dry_run / execute
    if let Some(format) = args.explain {
        let compiled_plan = pipeline_config.compile().expect("compile");
        let (dag, _) = PipelineExecutor::explain_plan_dag(&compiled_plan)?;
        match format {
            ExplainFormat::Text => {
                print!("{}", dag.explain_text(&pipeline_config));
            }
            ExplainFormat::Json => {
                let json = serde_json::to_string_pretty(&dag).map_err(|e| {
                    PipelineError::Config(clinker_core::config::ConfigError::Validation(format!(
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
            clinker_core::config::ConfigError::Validation(
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

    // Build runtime parameters
    let execution_id = uuid::Uuid::now_v7().to_string();
    let batch_id = args.resolved_batch_id();
    let pipeline_vars = pipeline_config
        .pipeline
        .vars
        .as_ref()
        .map(clinker_core::config::convert_pipeline_vars)
        .unwrap_or_default();
    let run_params = clinker_core::executor::PipelineRunParams {
        execution_id: execution_id.clone(),
        batch_id: batch_id.clone(),
        pipeline_vars,
        shutdown_token: None,
    };

    // Run the pipeline using file-based I/O
    let first_source = pipeline_config
        .source_configs()
        .next()
        .expect("pipeline has at least one source");
    let first_output = pipeline_config
        .output_configs()
        .next()
        .expect("pipeline has at least one output");
    let input_path = first_source.path.clone();
    let output_path = first_output.path.clone();
    let input_name = first_source.name.clone();
    let output_name = first_output.name.clone();

    let reader: Box<dyn std::io::Read + Send> = Box::new(std::fs::File::open(&input_path)?);
    let writer: Box<dyn std::io::Write + Send> = Box::new(std::fs::File::create(&output_path)?);

    let readers = std::collections::HashMap::from([(input_name, reader)]);
    let writers = std::collections::HashMap::from([(output_name, writer)]);

    let compiled_plan = pipeline_config.compile().expect("compile");
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &compiled_plan,
        readers,
        writers,
        &run_params,
    )?;

    let counters = &report.counters;
    let dlq_entries = &report.dlq_entries;

    // Write DLQ if there are entries and DLQ path is configured
    if !dlq_entries.is_empty()
        && let Some(ref dlq_config) = pipeline_config.error_handling.dlq
        && let Some(ref dlq_path) = dlq_config.path
    {
        let input_schema = {
            let f = std::fs::File::open(&input_path)?;
            let mut r = clinker_format::csv::reader::CsvReader::from_reader(
                f,
                clinker_format::csv::reader::CsvReaderConfig::default(),
            );
            r.schema().map_err(PipelineError::Format)?
        };
        let dlq_writer = std::fs::File::create(dlq_path)?;
        let include_reason = dlq_config.include_reason.unwrap_or(true);
        let include_source_row = dlq_config.include_source_row.unwrap_or(true);
        clinker_core::dlq::write_dlq(
            dlq_writer,
            dlq_entries,
            &input_schema,
            &input_path,
            include_reason,
            include_source_row,
        )
        .map_err(PipelineError::Format)?;
    }

    tracing::info!(
        "Pipeline complete: {} total, {} ok, {} dlq",
        counters.total_count,
        counters.ok_count,
        counters.dlq_count
    );

    // Exit codes per spec §10.2
    let exit_code: u8 = if counters.dlq_count > 0 { 2 } else { 0 };

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
            execution_mode: report.execution_summary.clone(),
            peak_rss_bytes: report.peak_rss_bytes,
            thread_count: num_threads(args),
            input_files: pipeline_config
                .source_configs()
                .map(|i| i.path.clone())
                .collect(),
            output_files: pipeline_config
                .output_configs()
                .map(|o| o.path.clone())
                .collect(),
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

/// Best-effort hostname for the metrics payload.
fn hostname_string() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| {
            // Read from /etc/hostname on Linux
            std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|_| "unknown".to_string())
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
