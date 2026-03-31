//! Execution metrics spool — write-then-rename pattern.
//!
//! Each pipeline execution writes a single JSON file to a configurable spool
//! directory using an atomic write-then-rename strategy:
//!
//! 1. Serialize [`ExecutionMetrics`] to `<spool_dir>/<execution_id>.json.tmp`
//! 2. `fsync()` the file
//! 3. `rename()` to `<spool_dir>/<execution_id>.json`
//!
//! This guarantees the collector never sees a partially-written file. If the
//! write fails, the caller should log all metric fields inline via `tracing::warn!`
//! so they are captured by the scheduler's stderr.
//!
//! The `collect_spool` function provides the sweep-and-append logic used by
//! `clinker metrics collect`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Versioned execution metrics payload written to the spool directory.
///
/// `schema_version` allows the collector to detect and skip incompatible files
/// written by older or newer versions of clinker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMetrics {
    /// UUID v7 identifying this execution. Reuses `RecordProvenance.source_batch`.
    pub execution_id: String,
    /// Schema version for forward/backward compatibility. Currently `1`.
    pub schema_version: u32,
    /// Pipeline name from `pipeline.name` in the YAML config.
    pub pipeline_name: String,
    /// Absolute path to the YAML config file that was executed.
    pub config_path: String,
    /// Hostname of the server that ran the pipeline.
    pub hostname: String,
    /// Wall-clock time when `run_with_readers_writers` was entered (UTC).
    pub started_at: DateTime<Utc>,
    /// Wall-clock time when the pipeline finished (UTC).
    pub finished_at: DateTime<Utc>,
    /// Duration in milliseconds (`finished_at - started_at`).
    pub duration_ms: i64,
    /// Process exit code (0 = clean, 1 = config error, 2 = partial, 3 = fatal, 4 = I/O).
    pub exit_code: u8,
    /// Total records read from the primary source.
    pub records_total: u64,
    /// Records successfully written to the output.
    pub records_ok: u64,
    /// Records routed to the DLQ.
    pub records_dlq: u64,
    /// Execution mode: `"Streaming"` or `"TwoPass"`.
    pub execution_mode: String,
    /// Peak process RSS in bytes observed across chunk boundaries.
    /// Measured on Linux, macOS, and Windows. `null` only on unsupported
    /// platforms (e.g., FreeBSD) where `rss_bytes()` returns `None`.
    pub peak_rss_bytes: Option<u64>,
    /// Thread pool size used during Phase 2 parallel evaluation.
    pub thread_count: usize,
    /// Source file paths (from `inputs[*].path` in config).
    pub input_files: Vec<String>,
    /// Output file paths (from `outputs[*].path` in config).
    pub output_files: Vec<String>,
    /// DLQ output path, if one was configured and written.
    pub dlq_path: Option<String>,
    /// Top-level error message if the pipeline exited with code 1, 3, or 4.
    /// `null` on clean success (exit 0) or partial success (exit 2).
    pub error: Option<String>,
}

/// Atomically write `metrics` to `<spool_dir>/<execution_id>.json`.
///
/// Writes to a `.tmp` file first, calls `fsync()`, then renames to the final
/// path. The collector only processes files without a `.tmp` suffix, so it
/// never observes a partially-written record.
///
/// Returns `Err` only if the spool write itself fails (e.g., NFS unavailable,
/// disk full). Callers should catch this error and emit a `tracing::warn!`
/// with metric fields inline — do not propagate it as a pipeline failure.
pub fn write_spool(metrics: &ExecutionMetrics, spool_dir: &Path) -> io::Result<()> {
    let id = &metrics.execution_id;
    let tmp_path = spool_dir.join(format!("{id}.json.tmp"));
    let final_path = spool_dir.join(format!("{id}.json"));

    let file = File::create(&tmp_path)?;
    let mut writer = BufWriter::new(&file);
    serde_json::to_writer(&mut writer, metrics)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    writer.flush()?;
    file.sync_all()?;
    drop(writer);
    fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// One entry produced by `collect_spool` — a parsed file and its path.
pub struct SpoolEntry {
    /// Path to the spool file (used for deletion by the caller if desired).
    pub path: PathBuf,
    /// Parsed metrics from the file.
    pub metrics: ExecutionMetrics,
}

/// Sweep `spool_dir` for completed `.json` files (skipping `.tmp`).
///
/// Returns an iterator of `(path, ExecutionMetrics)` pairs. Files that fail
/// to parse are skipped with a `tracing::warn!` — the collector never
/// corrupts its output by writing a partially valid record.
///
/// Caller is responsible for deletion after successful collection:
/// ```rust,ignore
/// for entry in collect_spool(&spool_dir)? {
///     append_to_ndjson(&mut output, &entry.metrics)?;
///     fs::remove_file(&entry.path)?;
/// }
/// ```
pub fn collect_spool(spool_dir: &Path) -> io::Result<impl Iterator<Item = SpoolEntry>> {
    let mut entries: Vec<SpoolEntry> = Vec::new();

    for dir_entry in fs::read_dir(spool_dir)? {
        let dir_entry = dir_entry?;
        let path = dir_entry.path();

        // Only process completed (non-tmp) JSON files
        match path.extension().and_then(|e| e.to_str()) {
            Some("json") => {}
            _ => continue,
        }

        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "metrics collect: failed to open spool file");
                continue;
            }
        };

        let metrics: ExecutionMetrics = match serde_json::from_reader(file) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "metrics collect: failed to parse spool file — skipping");
                continue;
            }
        };

        if metrics.schema_version != 1 {
            tracing::warn!(
                path = %path.display(),
                schema_version = metrics.schema_version,
                "metrics collect: unsupported schema_version — skipping"
            );
            continue;
        }

        entries.push(SpoolEntry { path, metrics });
    }

    // Sort by execution_id (UUID v7 = time-ordered) for deterministic output
    entries.sort_by(|a, b| a.metrics.execution_id.cmp(&b.metrics.execution_id));
    Ok(entries.into_iter())
}

/// Append `metrics` as one NDJSON line to `output_path` (opened in append mode).
pub fn append_ndjson(metrics: &ExecutionMetrics, output_path: &Path) -> io::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, metrics)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    writer.write_all(b"\n")?;
    writer.flush()
}

/// Resolve the effective spool directory from the three config sources.
///
/// Precedence (highest → lowest):
/// 1. `cli_flag` — `--metrics-spool-dir` CLI argument
/// 2. `CLINKER_METRICS_SPOOL_DIR` environment variable
/// 3. `yaml_spool_dir` — `pipeline.metrics.spool_dir` from the YAML config
///
/// Returns `None` if none of the three sources is set (metrics disabled).
pub fn resolve_spool_dir(
    cli_flag: Option<&Path>,
    yaml_spool_dir: Option<&str>,
) -> Option<PathBuf> {
    if let Some(p) = cli_flag {
        return Some(p.to_path_buf());
    }
    if let Ok(env) = std::env::var("CLINKER_METRICS_SPOOL_DIR") {
        if !env.is_empty() {
            return Some(PathBuf::from(env));
        }
    }
    yaml_spool_dir.map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::TempDir;

    fn sample_metrics(id: &str) -> ExecutionMetrics {
        let now = Utc::now();
        ExecutionMetrics {
            execution_id: id.to_string(),
            schema_version: 1,
            pipeline_name: "test-pipeline".into(),
            config_path: "/tmp/pipeline.yaml".into(),
            hostname: "test-host".into(),
            started_at: now,
            finished_at: now,
            duration_ms: 1234,
            exit_code: 0,
            records_total: 100,
            records_ok: 99,
            records_dlq: 1,
            execution_mode: "Streaming".into(),
            peak_rss_bytes: Some(52_428_800),
            thread_count: 4,
            input_files: vec!["input.csv".into()],
            output_files: vec!["output.csv".into()],
            dlq_path: Some("dlq.csv".into()),
            error: None,
        }
    }

    #[test]
    fn test_write_spool_atomic() {
        let dir = TempDir::new().unwrap();
        let id = "019571b2-0000-7000-0000-000000000001";
        let metrics = sample_metrics(id);

        write_spool(&metrics, dir.path()).unwrap();

        // Final file exists
        let final_path = dir.path().join(format!("{id}.json"));
        assert!(final_path.exists(), "final .json file should exist");

        // Temp file was cleaned up
        let tmp_path = dir.path().join(format!("{id}.json.tmp"));
        assert!(!tmp_path.exists(), ".tmp file should be removed after rename");
    }

    #[test]
    fn test_write_spool_roundtrip() {
        let dir = TempDir::new().unwrap();
        let id = "019571b2-0000-7000-0000-000000000002";
        let metrics = sample_metrics(id);

        write_spool(&metrics, dir.path()).unwrap();

        let final_path = dir.path().join(format!("{id}.json"));
        let file = File::open(&final_path).unwrap();
        let parsed: ExecutionMetrics = serde_json::from_reader(file).unwrap();

        assert_eq!(parsed.execution_id, id);
        assert_eq!(parsed.schema_version, 1);
        assert_eq!(parsed.pipeline_name, "test-pipeline");
        assert_eq!(parsed.records_total, 100);
        assert_eq!(parsed.records_ok, 99);
        assert_eq!(parsed.records_dlq, 1);
        assert_eq!(parsed.exit_code, 0);
        assert_eq!(parsed.peak_rss_bytes, Some(52_428_800));
    }

    #[test]
    fn test_write_spool_missing_dir_returns_error() {
        let bad_dir = PathBuf::from("/nonexistent/spool/path");
        let metrics = sample_metrics("019571b2-0000-7000-0000-000000000003");
        let result = write_spool(&metrics, &bad_dir);
        assert!(result.is_err(), "write_spool to non-existent dir should return Err");
    }

    #[test]
    fn test_collect_spool_roundtrip() {
        let dir = TempDir::new().unwrap();
        let id1 = "019571b2-0000-7000-0000-000000000010";
        let id2 = "019571b2-0000-7000-0000-000000000011";

        write_spool(&sample_metrics(id1), dir.path()).unwrap();
        write_spool(&sample_metrics(id2), dir.path()).unwrap();

        let entries: Vec<SpoolEntry> = collect_spool(dir.path()).unwrap().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].metrics.execution_id, id1);
        assert_eq!(entries[1].metrics.execution_id, id2);
    }

    #[test]
    fn test_collect_spool_skips_tmp_files() {
        let dir = TempDir::new().unwrap();
        // Write a .tmp file (simulates an interrupted write)
        let tmp_path = dir.path().join("orphan.json.tmp");
        fs::write(&tmp_path, b"{}").unwrap();

        let entries: Vec<SpoolEntry> = collect_spool(dir.path()).unwrap().collect();
        assert_eq!(entries.len(), 0, "orphaned .tmp files should be skipped");
    }

    #[test]
    fn test_append_ndjson() {
        let dir = TempDir::new().unwrap();
        let output_path = dir.path().join("metrics.ndjson");
        let m1 = sample_metrics("019571b2-0000-7000-0000-000000000020");
        let m2 = sample_metrics("019571b2-0000-7000-0000-000000000021");

        append_ndjson(&m1, &output_path).unwrap();
        append_ndjson(&m2, &output_path).unwrap();

        let content = fs::read_to_string(&output_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "should have written 2 NDJSON lines");

        let parsed1: ExecutionMetrics = serde_json::from_str(lines[0]).unwrap();
        let parsed2: ExecutionMetrics = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed1.execution_id, "019571b2-0000-7000-0000-000000000020");
        assert_eq!(parsed2.execution_id, "019571b2-0000-7000-0000-000000000021");
    }

    #[test]
    fn test_resolve_spool_dir_cli_wins() {
        unsafe { std::env::remove_var("CLINKER_METRICS_SPOOL_DIR") };
        let cli_path = Path::new("/cli/path");
        let result = resolve_spool_dir(Some(cli_path), Some("/yaml/path"));
        assert_eq!(result, Some(PathBuf::from("/cli/path")));
    }

    #[test]
    fn test_resolve_spool_dir_env_over_yaml() {
        unsafe { std::env::set_var("CLINKER_METRICS_SPOOL_DIR", "/env/path") };
        let result = resolve_spool_dir(None, Some("/yaml/path"));
        assert_eq!(result, Some(PathBuf::from("/env/path")));
        unsafe { std::env::remove_var("CLINKER_METRICS_SPOOL_DIR") };
    }

    #[test]
    fn test_resolve_spool_dir_yaml_fallback() {
        unsafe { std::env::remove_var("CLINKER_METRICS_SPOOL_DIR") };
        let result = resolve_spool_dir(None, Some("/yaml/path"));
        assert_eq!(result, Some(PathBuf::from("/yaml/path")));
    }

    #[test]
    fn test_resolve_spool_dir_none_when_all_absent() {
        unsafe { std::env::remove_var("CLINKER_METRICS_SPOOL_DIR") };
        let result = resolve_spool_dir(None, None);
        assert_eq!(result, None);
    }
}
