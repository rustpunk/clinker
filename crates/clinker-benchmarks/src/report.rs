//! Benchmark reporting: human-readable summary table and CI-friendly JSON.
//!
//! The summary table is printed to stdout when `CLINKER_BENCH_SUMMARY=1`.
//! The CI JSON is written to `target/bench-results/summary.json`.

use std::fmt::Write as FmtWrite;
use std::path::Path;

use clinker_bench_support::ConfigEntry;
use clinker_core::executor::ExecutionReport;
use serde::Serialize;

// ── Summary table ────────────────────────────────────────────────

/// Format a human-readable summary table for a benchmark run.
///
/// Default: 5 columns (Stage, Elapsed, Rows In, Rows Out, RSS After, ~72 chars).
/// Set `CLINKER_BENCH_VERBOSE=1` for full 9-column output with CPU + IO.
pub fn format_summary_table(pipeline_name: &str, scale: &str, report: &ExecutionReport) -> String {
    let verbose = std::env::var("CLINKER_BENCH_VERBOSE").is_ok();
    let mut out = String::new();

    // Header
    let _ = writeln!(out, "\u{250c} {pipeline_name} [{scale}]");
    let _ = writeln!(out, "\u{2502}");

    // Column headers
    if verbose {
        let _ = writeln!(
            out,
            "\u{2502} {:>18} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
            "Stage",
            "Elapsed",
            "Rows In",
            "Rows Out",
            "RSS After",
            "CPU User",
            "CPU Sys",
            "IO Read",
            "IO Write"
        );
    } else {
        let _ = writeln!(
            out,
            "\u{2502} {:>18} {:>10} {:>10} {:>10} {:>10}",
            "Stage", "Elapsed", "Rows In", "Rows Out", "RSS After"
        );
    }

    {
        let sep18 = "\u{2500}".repeat(18);
        let sep10 = "\u{2500}".repeat(10);
        if verbose {
            let _ = writeln!(
                out,
                "\u{2502} {:>18} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
                sep18, sep10, sep10, sep10, sep10, sep10, sep10, sep10, sep10,
            );
        } else {
            let _ = writeln!(
                out,
                "\u{2502} {:>18} {:>10} {:>10} {:>10} {:>10}",
                sep18, sep10, sep10, sep10, sep10,
            );
        }
    }

    // Per-stage rows
    let mut total_elapsed = std::time::Duration::ZERO;
    let mut total_records_in: u64 = 0;
    let mut total_records_out: u64 = 0;
    let mut peak_rss: Option<u64> = None;

    for stage in &report.stages {
        total_elapsed += stage.elapsed;
        total_records_in += stage.records_in;
        total_records_out += stage.records_out;
        if let Some(rss) = stage.rss_after {
            peak_rss = Some(peak_rss.map_or(rss, |p: u64| p.max(rss)));
        }

        let name = stage.name.to_string();
        let elapsed = format_duration(stage.elapsed);
        let rows_in = format_count(stage.records_in);
        let rows_out = format_count(stage.records_out);
        let rss = stage.rss_after.map_or("-".to_string(), format_bytes);

        if verbose {
            let cpu_user = stage
                .cpu_user_delta_ns
                .map_or("-".to_string(), format_duration_ns);
            let cpu_sys = stage
                .cpu_sys_delta_ns
                .map_or("-".to_string(), format_duration_ns);
            let io_read = stage.io_read_delta.map_or("-".to_string(), format_bytes);
            let io_write = stage.io_write_delta.map_or("-".to_string(), format_bytes);
            let _ = writeln!(
                out,
                "\u{2502} {:>18} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
                name, elapsed, rows_in, rows_out, rss, cpu_user, cpu_sys, io_read, io_write
            );
        } else {
            let _ = writeln!(
                out,
                "\u{2502} {:>18} {:>10} {:>10} {:>10} {:>10}",
                name, elapsed, rows_in, rows_out, rss
            );
        }
    }

    // Total row
    let total_wall_ns = (report.finished_at - report.started_at)
        .num_nanoseconds()
        .filter(|&ns| ns >= 0)
        .unwrap_or(0) as u128;
    let total_wall = std::time::Duration::from_nanos(total_wall_ns as u64);

    if verbose {
        let total_cpu_user = report
            .total_cpu_user_ns
            .map_or("-".to_string(), format_duration_ns);
        let total_cpu_sys = report
            .total_cpu_sys_ns
            .map_or("-".to_string(), format_duration_ns);
        let total_io_read = report
            .total_io_read_bytes
            .map_or("-".to_string(), format_bytes);
        let total_io_write = report
            .total_io_write_bytes
            .map_or("-".to_string(), format_bytes);
        let _ = writeln!(
            out,
            "\u{2502} {:>18} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
            "TOTAL",
            format_duration(total_wall),
            format_count(total_records_in),
            format_count(total_records_out),
            peak_rss.map_or("-".to_string(), format_bytes),
            total_cpu_user,
            total_cpu_sys,
            total_io_read,
            total_io_write,
        );
    } else {
        let _ = writeln!(
            out,
            "\u{2502} {:>18} {:>10} {:>10} {:>10} {:>10}",
            "TOTAL",
            format_duration(total_wall),
            format_count(total_records_in),
            format_count(total_records_out),
            peak_rss.map_or("-".to_string(), format_bytes),
        );
    }

    // Footer: throughput + CPU util
    let total_ms = total_wall.as_secs_f64() * 1000.0;
    let rps = if total_ms > 0.0 {
        report.counters.total_count as f64 / total_ms * 1000.0
    } else {
        0.0
    };

    let _ = writeln!(out, "\u{2502}");
    let _ = writeln!(out, "\u{2502} Throughput: {:.0} records/sec", rps);

    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    if let (Some(u), Some(s)) = (report.total_cpu_user_ns, report.total_cpu_sys_ns) {
        let total_cpu_ns = u + s;
        let elapsed_ns = total_wall.as_nanos() as f64;
        if elapsed_ns > 0.0 {
            let util = total_cpu_ns as f64 / elapsed_ns / num_cpus as f64 * 100.0;
            let _ = writeln!(
                out,
                "\u{2502} CPU util: {:.1}% (across {} cores)",
                util, num_cpus
            );
        }
    }

    if !verbose {
        let _ = writeln!(
            out,
            "\u{2502} (set CLINKER_BENCH_VERBOSE=1 for CPU + IO breakdown per stage)"
        );
    }

    // Sanity: cumulative stage elapsed should not exceed total wall time.
    // Use a soft warning rather than assert (clock sources may differ slightly).
    let stage_sum_ns: u128 = report.stages.iter().map(|s| s.elapsed.as_nanos()).sum();
    if stage_sum_ns > total_wall_ns {
        let _ = writeln!(
            out,
            "\u{2502} WARNING: cumulative stage elapsed ({stage_sum_ns}ns) exceeds total ({total_wall_ns}ns)"
        );
    }

    let _ = writeln!(out, "\u{2514}");
    out
}

/// Print a human-readable summary table to stdout.
pub fn print_summary_table(pipeline_name: &str, scale: &str, report: &ExecutionReport) {
    print!("{}", format_summary_table(pipeline_name, scale, report));
}

// ── CI JSON ──────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct BenchReport {
    pub timestamp: String,
    pub git_sha: String,
    pub platform: String,
    pub num_cpus: usize,
    pub results: Vec<BenchResult>,
}

#[derive(Serialize, Clone)]
pub struct BenchResult {
    pub pipeline: String,
    pub scale: String,
    pub platform: String,
    pub total_elapsed_ms: f64,
    pub records_per_sec: f64,
    pub bytes_per_sec: Option<f64>,
    pub peak_rss_bytes: Option<u64>,
    pub total_cpu_user_ms: Option<f64>,
    pub total_cpu_sys_ms: Option<f64>,
    pub cpu_utilization_pct: Option<f64>,
    pub total_io_read_bytes: Option<u64>,
    pub total_io_write_bytes: Option<u64>,
    pub stages: Vec<StageResult>,
}

#[derive(Serialize, Clone)]
pub struct StageResult {
    pub name: String,
    pub elapsed_ms: f64,
    pub records_in: u64,
    pub records_out: u64,
    pub rss_after: Option<u64>,
    pub cpu_user_delta_ms: Option<f64>,
    pub cpu_sys_delta_ms: Option<f64>,
    pub io_read_delta: Option<u64>,
    pub io_write_delta: Option<u64>,
    pub heap_delta_bytes: Option<i64>,
    pub heap_alloc_count: Option<u64>,
}

/// Get the current git short SHA for benchmark metadata.
/// Checks `GITHUB_SHA` env var first (CI), falls back to git CLI.
pub fn git_short_sha() -> String {
    if let Ok(sha) = std::env::var("GITHUB_SHA") {
        return sha[..sha.len().min(8)].to_string();
    }
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Convert an `ExecutionReport` into a CI-friendly `BenchResult`.
pub fn bench_result_from(
    entry: &ConfigEntry,
    scale: &str,
    report: &ExecutionReport,
) -> BenchResult {
    let total_ms = (report.finished_at - report.started_at)
        .num_milliseconds()
        .max(0) as f64;
    let rps = if total_ms > 0.0 {
        report.counters.total_count as f64 / total_ms * 1000.0
    } else {
        0.0
    };
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let cpu_util = match (report.total_cpu_user_ns, report.total_cpu_sys_ns) {
        (Some(u), Some(s)) => {
            let total_cpu_ns = u + s;
            let elapsed_ns = total_ms * 1_000_000.0;
            if elapsed_ns > 0.0 {
                Some(total_cpu_ns as f64 / elapsed_ns / num_cpus as f64 * 100.0)
            } else {
                None
            }
        }
        _ => None,
    };

    BenchResult {
        pipeline: format!("{}/{}", entry.category, entry.name),
        scale: scale.to_string(),
        platform: std::env::consts::OS.to_string(),
        total_elapsed_ms: total_ms,
        records_per_sec: rps,
        bytes_per_sec: None,
        peak_rss_bytes: report.peak_rss_bytes,
        total_cpu_user_ms: report.total_cpu_user_ns.map(|ns| ns as f64 / 1_000_000.0),
        total_cpu_sys_ms: report.total_cpu_sys_ns.map(|ns| ns as f64 / 1_000_000.0),
        cpu_utilization_pct: cpu_util,
        total_io_read_bytes: report.total_io_read_bytes,
        total_io_write_bytes: report.total_io_write_bytes,
        stages: report
            .stages
            .iter()
            .map(|s| StageResult {
                name: s.name.to_string(),
                elapsed_ms: s.elapsed.as_secs_f64() * 1000.0,
                records_in: s.records_in,
                records_out: s.records_out,
                rss_after: s.rss_after,
                cpu_user_delta_ms: s.cpu_user_delta_ns.map(|ns| ns as f64 / 1_000_000.0),
                cpu_sys_delta_ms: s.cpu_sys_delta_ns.map(|ns| ns as f64 / 1_000_000.0),
                io_read_delta: s.io_read_delta,
                io_write_delta: s.io_write_delta,
                heap_delta_bytes: s.heap_delta_bytes,
                heap_alloc_count: s.heap_alloc_count,
            })
            .collect(),
    }
}

/// Write CI-friendly JSON benchmark results to disk.
pub fn write_ci_json(results: &[BenchResult], output_path: &Path) {
    let report = BenchReport {
        timestamp: chrono::Utc::now().to_rfc3339(),
        git_sha: git_short_sha(),
        platform: std::env::consts::OS.to_string(),
        num_cpus: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        results: results.to_vec(),
    };
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent).expect("create bench-results dir");
    }
    let json = serde_json::to_string_pretty(&report).expect("serialize");
    std::fs::write(output_path, json).expect("write CI JSON");
}

// ── Formatting helpers ───────────────────────────────────────────

fn format_duration(d: std::time::Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("{:.1}\u{00b5}s", ms * 1000.0)
    } else if ms < 1000.0 {
        format!("{:.1}ms", ms)
    } else {
        format!("{:.2}s", ms / 1000.0)
    }
}

fn format_duration_ns(ns: u64) -> String {
    format_duration(std::time::Duration::from_nanos(ns))
}

fn format_bytes(b: u64) -> String {
    if b < 1024 {
        format!("{}B", b)
    } else if b < 1024 * 1024 {
        format!("{:.1}KB", b as f64 / 1024.0)
    } else {
        format!("{:.1}MB", b as f64 / (1024.0 * 1024.0))
    }
}

fn format_count(n: u64) -> String {
    if n == 0 {
        "0".to_string()
    } else if n < 1_000 {
        n.to_string()
    } else if n < 1_000_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_core::executor::stage_metrics::{StageMetrics, StageName};
    use std::time::Duration;

    fn mock_report() -> ExecutionReport {
        let now = chrono::Utc::now();
        let started_at = now - chrono::Duration::milliseconds(60);
        // Build a report then set counter fields through the public API
        // to avoid naming PipelineCounters (lives in clinker-record, not a dep).
        let mut report = ExecutionReport {
            counters: Default::default(),
            dlq_entries: Vec::new(),
            execution_summary: "Streaming".to_string(),
            required_arena: false,
            required_sorted_input: false,
            peak_rss_bytes: Some(25_000_000),
            total_cpu_user_ns: Some(45_000_000),
            total_cpu_sys_ns: Some(3_000_000),
            total_io_read_bytes: Some(2_000_000),
            total_io_write_bytes: Some(1_800_000),
            started_at,
            finished_at: now,
            stages: vec![
                StageMetrics {
                    name: StageName::Compile,
                    elapsed: Duration::from_millis(1),
                    records_in: 0,
                    records_out: 0,
                    bytes_written: None,
                    rss_after: Some(19_000_000),
                    cpu_user_delta_ns: Some(800_000),
                    cpu_sys_delta_ns: Some(100_000),
                    io_read_delta: None,
                    io_write_delta: None,
                    heap_delta_bytes: None,
                    heap_alloc_count: None,
                },
                StageMetrics {
                    name: StageName::TransformEval,
                    elapsed: Duration::from_millis(40),
                    records_in: 1000,
                    records_out: 1000,
                    bytes_written: None,
                    rss_after: Some(22_000_000),
                    cpu_user_delta_ns: Some(35_000_000),
                    cpu_sys_delta_ns: Some(2_000_000),
                    io_read_delta: Some(500_000),
                    io_write_delta: Some(300_000),
                    heap_delta_bytes: None,
                    heap_alloc_count: None,
                },
                StageMetrics {
                    name: StageName::Write,
                    elapsed: Duration::from_millis(15),
                    records_in: 1000,
                    records_out: 1000,
                    bytes_written: Some(50_000),
                    rss_after: Some(25_000_000),
                    cpu_user_delta_ns: Some(9_000_000),
                    cpu_sys_delta_ns: Some(900_000),
                    io_read_delta: Some(100),
                    io_write_delta: Some(50_000),
                    heap_delta_bytes: None,
                    heap_alloc_count: None,
                },
            ],
        };
        report.counters.total_count = 1000;
        report.counters.ok_count = 995;
        report.counters.dlq_count = 5;
        report
    }

    /// Produces output without panicking for a valid mock report.
    #[test]
    fn test_summary_table_formats_without_panic() {
        let report = mock_report();
        let output = format_summary_table("test/csv_passthrough", "small", &report);
        assert!(!output.is_empty());
    }

    /// Each stage name appears in the output.
    #[test]
    fn test_summary_table_includes_all_stages() {
        let report = mock_report();
        let output = format_summary_table("test/csv_passthrough", "small", &report);
        assert!(output.contains("Compile"), "missing Compile");
        assert!(output.contains("TransformEval"), "missing TransformEval");
        assert!(output.contains("Write"), "missing Write");
    }

    /// Sanity warning fires when stages exceed total.
    #[test]
    fn test_cumulative_stage_sum_not_exceeding_total() {
        let now = chrono::Utc::now();
        // Total wall = 10ms, but stages sum to 200ms
        let report = ExecutionReport {
            counters: Default::default(),
            dlq_entries: Vec::new(),
            execution_summary: String::new(),
            required_arena: false,
            required_sorted_input: false,
            peak_rss_bytes: None,
            total_cpu_user_ns: None,
            total_cpu_sys_ns: None,
            total_io_read_bytes: None,
            total_io_write_bytes: None,
            started_at: now - chrono::Duration::milliseconds(10),
            finished_at: now,
            stages: vec![
                StageMetrics {
                    name: StageName::TransformEval,
                    elapsed: Duration::from_millis(100),
                    records_in: 0,
                    records_out: 0,
                    bytes_written: None,
                    rss_after: None,
                    cpu_user_delta_ns: None,
                    cpu_sys_delta_ns: None,
                    io_read_delta: None,
                    io_write_delta: None,
                    heap_delta_bytes: None,
                    heap_alloc_count: None,
                },
                StageMetrics {
                    name: StageName::Write,
                    elapsed: Duration::from_millis(100),
                    records_in: 0,
                    records_out: 0,
                    bytes_written: None,
                    rss_after: None,
                    cpu_user_delta_ns: None,
                    cpu_sys_delta_ns: None,
                    io_read_delta: None,
                    io_write_delta: None,
                    heap_delta_bytes: None,
                    heap_alloc_count: None,
                },
            ],
        };
        let output = format_summary_table("test/bad", "small", &report);
        assert!(
            output.contains("WARNING: cumulative stage elapsed"),
            "expected warning for stage sum > total"
        );
    }

    /// Serialized JSON parses back to the same structure.
    #[test]
    fn test_ci_json_serializes_valid_json() {
        let result = mock_bench_result();
        let json = serde_json::to_string(&result).unwrap();
        let _: serde_json::Value = serde_json::from_str(&json).unwrap();
    }

    /// File is created at the expected path.
    #[test]
    fn test_ci_json_writes_to_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bench-results/summary.json");
        write_ci_json(&[mock_bench_result()], &path);
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(parsed["results"].is_array());
        assert!(parsed["git_sha"].is_string());
        assert!(parsed["timestamp"].is_string());
    }

    fn mock_bench_result() -> BenchResult {
        BenchResult {
            pipeline: "format/csv_passthrough".to_string(),
            scale: "small".to_string(),
            platform: "linux".to_string(),
            total_elapsed_ms: 62.6,
            records_per_sec: 1_595_527.0,
            bytes_per_sec: None,
            peak_rss_bytes: Some(25_378_816),
            total_cpu_user_ms: Some(45.2),
            total_cpu_sys_ms: Some(3.1),
            cpu_utilization_pct: Some(12.0),
            total_io_read_bytes: Some(2_000_000),
            total_io_write_bytes: Some(1_800_000),
            stages: vec![StageResult {
                name: "Compile".to_string(),
                elapsed_ms: 1.2,
                records_in: 0,
                records_out: 0,
                rss_after: Some(19_293_184),
                cpu_user_delta_ms: Some(0.8),
                cpu_sys_delta_ms: Some(0.1),
                io_read_delta: None,
                io_write_delta: None,
                heap_delta_bytes: None,
                heap_alloc_count: None,
            }],
        }
    }
}
