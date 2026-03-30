use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use clinker_core::config;
use clinker_core::error::PipelineError;
use clinker_core::executor::PipelineExecutor;
use clinker_core::pipeline::memory::parse_memory_limit_bytes;
use clinker_format::FormatReader;

/// CXL streaming ETL engine.
#[derive(Parser, Debug)]
#[command(name = "clinker", about = "CXL streaming ETL engine")]
pub struct Cli {
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
}

impl Cli {
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

fn main() -> ExitCode {
    let cli = Cli::parse();

    // Initialize tracing subscriber
    let filter = cli.log_level.parse::<tracing_subscriber::filter::LevelFilter>()
        .unwrap_or(tracing_subscriber::filter::LevelFilter::INFO);
    tracing_subscriber::fmt()
        .with_max_level(filter)
        .init();

    match run(&cli) {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            tracing::error!("{e}");
            match &e {
                PipelineError::Config(_) | PipelineError::Compilation { .. } => ExitCode::from(1),
                PipelineError::Io(_) => ExitCode::from(4),
                PipelineError::Eval(_) => ExitCode::from(3),
                PipelineError::Format(_) | PipelineError::ThreadPool(_) => ExitCode::from(4),
            }
        }
    }
}

fn run(cli: &Cli) -> Result<u8, PipelineError> {
    // Load config
    let pipeline_config = config::load_config(&cli.config)?;

    if cli.explain {
        let plan_output = PipelineExecutor::explain(&pipeline_config)?;
        println!("{}", plan_output);
        return Ok(0);
    }

    if cli.dry_run {
        tracing::info!("Dry run: config valid, {} inputs, {} outputs, {} transforms",
            pipeline_config.inputs.len(),
            pipeline_config.outputs.len(),
            pipeline_config.transformations.len(),
        );
        return Ok(0);
    }

    // Run the pipeline using file-based I/O
    let input_path = &pipeline_config.inputs[0].path;
    let output_path = &pipeline_config.outputs[0].path;

    let reader = std::fs::File::open(input_path)?;
    let writer = std::fs::File::create(output_path)?;

    let (counters, dlq_entries) =
        PipelineExecutor::run_with_readers_writers(&pipeline_config, reader, writer)?;

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
                    &dlq_entries,
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
    if counters.dlq_count > 0 {
        Ok(2) // Partial success
    } else {
        Ok(0) // Clean success
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_cli_positional_config_path() {
        let cli = Cli::try_parse_from(["clinker", "pipeline.yaml"]).unwrap();
        assert_eq!(cli.config, PathBuf::from("pipeline.yaml"));
        assert!(!cli.dry_run);
    }

    #[test]
    fn test_cli_dry_run_flag() {
        let cli = Cli::try_parse_from(["clinker", "--dry-run", "pipeline.yaml"]).unwrap();
        assert!(cli.dry_run);
    }

    #[test]
    fn test_cli_log_level_default() {
        let cli = Cli::try_parse_from(["clinker", "pipeline.yaml"]).unwrap();
        assert_eq!(cli.log_level, "info");
    }

    // ── Phase 8 Task 8.4 gate tests ─────────────────────────────

    #[test]
    fn test_cli_memory_limit_suffix_k() {
        let cli = Cli::try_parse_from(["clinker", "--memory-limit", "512K", "p.yaml"]).unwrap();
        assert_eq!(cli.memory_limit_bytes(), 524288);
    }

    #[test]
    fn test_cli_memory_limit_suffix_m() {
        let cli = Cli::try_parse_from(["clinker", "--memory-limit", "256M", "p.yaml"]).unwrap();
        assert_eq!(cli.memory_limit_bytes(), 268435456);
    }

    #[test]
    fn test_cli_memory_limit_suffix_g() {
        let cli = Cli::try_parse_from(["clinker", "--memory-limit", "2G", "p.yaml"]).unwrap();
        assert_eq!(cli.memory_limit_bytes(), 2147483648);
    }

    #[test]
    fn test_cli_memory_limit_bare_bytes() {
        let cli = Cli::try_parse_from(["clinker", "--memory-limit", "1000000", "p.yaml"]).unwrap();
        assert_eq!(cli.memory_limit_bytes(), 1000000);
    }

    #[test]
    fn test_cli_default_memory_limit() {
        let cli = Cli::try_parse_from(["clinker", "p.yaml"]).unwrap();
        assert_eq!(cli.memory_limit_bytes(), 256 * 1024 * 1024); // 256MB
    }

    #[test]
    fn test_cli_error_threshold_zero() {
        let cli = Cli::try_parse_from(["clinker", "p.yaml"]).unwrap();
        assert_eq!(cli.error_threshold, 0); // 0 = unlimited
    }

    #[test]
    fn test_cli_batch_id_default_uuid() {
        let cli = Cli::try_parse_from(["clinker", "p.yaml"]).unwrap();
        assert!(cli.batch_id.is_none());
        let id = cli.resolved_batch_id();
        // Should be a valid UUID
        uuid::Uuid::parse_str(&id).expect("default batch_id should be valid UUID");
    }

    #[test]
    fn test_cli_quiet_flag() {
        let cli = Cli::try_parse_from(["clinker", "--quiet", "p.yaml"]).unwrap();
        assert!(cli.quiet);
    }

    #[test]
    fn test_cli_force_flag() {
        let cli = Cli::try_parse_from(["clinker", "--force", "p.yaml"]).unwrap();
        assert!(cli.force);
    }
}
