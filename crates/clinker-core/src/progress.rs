//! Progress reporting for pipeline execution.
//!
//! `ProgressReporter` is a callback trait invoked at chunk boundaries.
//! `StderrReporter` throttles output to 1 update/sec.
//! `NullReporter` is a no-op for `--quiet` mode.

use std::fmt::Write as FmtWrite;
use std::io::Write;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Progress update emitted at chunk boundaries.
#[derive(Debug, Clone)]
pub struct ProgressUpdate {
    pub phase: String,
    pub file: String,
    pub processed: u64,
    pub total: Option<u64>,
    pub elapsed: Duration,
}

impl ProgressUpdate {
    /// Format as spec §10.5: `[cxl] file: Phase N name... X/Y records (Z%) [T]`
    pub fn format(&self) -> String {
        let mut s = format!("[cxl] {}: {}... ", self.file, self.phase);
        if let Some(total) = self.total {
            let pct = if total > 0 {
                (self.processed as f64 / total as f64 * 100.0) as u64
            } else {
                0
            };
            write!(s, "{}/{} records ({}%) ", self.processed, total, pct).unwrap();
        } else {
            write!(s, "{} records ", self.processed).unwrap();
        }
        write!(s, "[{:.1}s]", self.elapsed.as_secs_f64()).unwrap();
        s
    }
}

/// Callback trait for progress reporting. Testable via VecReporter.
pub trait ProgressReporter: Send + Sync {
    fn report(&self, update: &ProgressUpdate);
}

/// Writes progress to stderr, throttled to 1 update per second.
pub struct StderrReporter {
    last_report: Mutex<Instant>,
}

impl StderrReporter {
    pub fn new() -> Self {
        Self {
            last_report: Mutex::new(Instant::now() - Duration::from_secs(2)),
        }
    }
}

impl ProgressReporter for StderrReporter {
    fn report(&self, update: &ProgressUpdate) {
        let mut last = self.last_report.lock().unwrap();
        if last.elapsed() >= Duration::from_secs(1) {
            eprintln!("{}", update.format());
            *last = Instant::now();
        }
    }
}

/// No-op reporter for `--quiet` mode.
pub struct NullReporter;

impl ProgressReporter for NullReporter {
    fn report(&self, _update: &ProgressUpdate) {}
}

/// Collects all updates for testing.
#[cfg(any(test, feature = "test-utils"))]
pub struct VecReporter {
    pub updates: Mutex<Vec<ProgressUpdate>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl VecReporter {
    pub fn new() -> Self {
        Self {
            updates: Mutex::new(Vec::new()),
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl ProgressReporter for VecReporter {
    fn report(&self, update: &ProgressUpdate) {
        self.updates.lock().unwrap().push(update.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stderr_reporter_throttle() {
        let reporter = StderrReporter::new();
        let update = ProgressUpdate {
            phase: "Phase 1 indexing".into(),
            file: "test.csv".into(),
            processed: 100,
            total: Some(1000),
            elapsed: Duration::from_secs(1),
        };

        // First report should go through (initialized 2 seconds in the past)
        reporter.report(&update);
        let t1 = *reporter.last_report.lock().unwrap();

        // Immediate second report should be throttled (no update to last_report)
        let update2 = ProgressUpdate {
            processed: 200,
            ..update.clone()
        };
        reporter.report(&update2);
        let t2 = *reporter.last_report.lock().unwrap();
        assert_eq!(t1, t2, "second report within 1 second should be throttled");
    }

    #[test]
    fn test_stderr_reporter_format_with_total() {
        let update = ProgressUpdate {
            phase: "Phase 2 transforming".into(),
            file: "orders.csv".into(),
            processed: 150000,
            total: Some(500000),
            elapsed: Duration::from_secs_f64(12.3),
        };
        let formatted = update.format();
        assert!(formatted.contains("[cxl] orders.csv:"));
        assert!(formatted.contains("Phase 2 transforming"));
        assert!(formatted.contains("150000/500000 records"));
        assert!(formatted.contains("30%"));
        assert!(formatted.contains("[12.3s]"));
    }

    #[test]
    fn test_stderr_reporter_format_without_total() {
        let update = ProgressUpdate {
            phase: "Phase 2 transforming".into(),
            file: "stream.csv".into(),
            processed: 50000,
            total: None,
            elapsed: Duration::from_secs_f64(5.0),
        };
        let formatted = update.format();
        assert!(formatted.contains("50000 records"));
        assert!(!formatted.contains('/'));
        assert!(!formatted.contains('%'));
    }

    #[test]
    fn test_null_reporter_silent() {
        let reporter = NullReporter;
        let update = ProgressUpdate {
            phase: "Phase 1 indexing".into(),
            file: "test.csv".into(),
            processed: 100,
            total: Some(1000),
            elapsed: Duration::from_secs(1),
        };
        // Should not panic or produce any output
        reporter.report(&update);
    }

    #[test]
    fn test_vec_reporter_collects() {
        let reporter = VecReporter::new();
        let update = ProgressUpdate {
            phase: "Phase 1 indexing".into(),
            file: "test.csv".into(),
            processed: 100,
            total: Some(1000),
            elapsed: Duration::from_secs(1),
        };
        reporter.report(&update);
        reporter.report(&update);
        let updates = reporter.updates.lock().unwrap();
        assert_eq!(updates.len(), 2);
    }

    #[test]
    fn test_progress_update_phase_specific() {
        let p1 = ProgressUpdate {
            phase: "Phase 1 indexing".into(),
            file: "test.csv".into(),
            processed: 100,
            total: Some(1000),
            elapsed: Duration::from_secs(1),
        };
        let p2 = ProgressUpdate {
            phase: "Phase 2 transforming".into(),
            file: "test.csv".into(),
            processed: 50,
            total: Some(1000),
            elapsed: Duration::from_secs(2),
        };
        assert!(p1.format().contains("Phase 1 indexing"));
        assert!(p2.format().contains("Phase 2 transforming"));
    }
}
