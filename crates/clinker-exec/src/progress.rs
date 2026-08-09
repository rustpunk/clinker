//! Progress reporting for pipeline execution.
//!
//! `ProgressReporter` is a callback trait invoked at chunk boundaries.
//! `StderrReporter` throttles output to 1 update/sec.
//! `NullReporter` is a no-op for `--quiet` mode.

use std::fmt::Write as FmtWrite;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const DEFAULT_MAX_MACHINE_EVENTS: usize = 128;
const DEFAULT_MAX_DETAIL_BYTES: usize = 64;

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

/// Whether a machine progress record marks a lifecycle edge or an advisory
/// periodic observation inside one phase.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ProgressKind {
    Transition,
    Periodic,
}

impl ProgressKind {
    /// Stable schema-1 spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transition => "transition",
            Self::Periodic => "periodic",
        }
    }
}

/// Sanitized, bounded machine-facing progress state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgressSnapshot {
    phase: String,
    kind: ProgressKind,
    elapsed: Duration,
    detail_truncated: bool,
    event_limit_reached: bool,
}

impl ProgressSnapshot {
    pub fn phase(&self) -> &str {
        &self.phase
    }

    pub const fn kind(&self) -> ProgressKind {
        self.kind
    }

    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    pub const fn detail_truncated(&self) -> bool {
        self.detail_truncated
    }

    pub const fn event_limit_reached(&self) -> bool {
        self.event_limit_reached
    }
}

/// Coalesces discardable observations before they reach the machine writer.
///
/// Required transitions are always returned. Periodic snapshots appear at
/// most once per second, with 128 ordinary records and one explicit cap
/// notification by default. Logical detail is truncated on UTF-8 boundaries.
pub struct BoundedProgress {
    started: Instant,
    last_periodic: Option<Instant>,
    periodic_emitted: usize,
    max_periodic_events: usize,
    max_detail_bytes: usize,
    periodic_limit_reported: bool,
}

impl Default for BoundedProgress {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_MACHINE_EVENTS, DEFAULT_MAX_DETAIL_BYTES)
    }
}

impl BoundedProgress {
    pub fn new(max_periodic_events: usize, max_detail_bytes: usize) -> Self {
        Self {
            started: Instant::now(),
            last_periodic: None,
            periodic_emitted: 0,
            max_periodic_events,
            max_detail_bytes,
            periodic_limit_reported: false,
        }
    }

    pub fn transition(&mut self, phase: &str) -> ProgressSnapshot {
        self.snapshot(phase, ProgressKind::Transition, Instant::now(), false)
    }

    pub fn periodic(&mut self, phase: &str) -> Option<ProgressSnapshot> {
        self.periodic_at(phase, Instant::now())
    }

    /// Deterministic-time form used by focused cadence tests.
    pub fn periodic_at(&mut self, phase: &str, now: Instant) -> Option<ProgressSnapshot> {
        if self
            .last_periodic
            .is_some_and(|last| now.duration_since(last) < Duration::from_secs(1))
        {
            return None;
        }
        self.last_periodic = Some(now);
        if self.periodic_emitted >= self.max_periodic_events {
            if self.periodic_limit_reported {
                return None;
            }
            self.periodic_limit_reported = true;
            return Some(self.snapshot(phase, ProgressKind::Periodic, now, true));
        }
        self.periodic_emitted = self.periodic_emitted.saturating_add(1);
        Some(self.snapshot(phase, ProgressKind::Periodic, now, false))
    }

    /// Give back the one-shot event-limit notice for a record that may not
    /// have arrived.
    ///
    /// The flag is spent when the snapshot is handed out, not when a reader
    /// sees it. Without this, one failed write of the cap notice silenced the
    /// stream for the rest of the run with nothing saying why: every later
    /// call returns `None`, so the record explaining the silence was the one
    /// record lost. The caller's own retrying is bounded, so re-offering it
    /// cannot go on forever.
    pub fn restore_event_limit_notice(&mut self) {
        self.periodic_limit_reported = false;
    }

    fn snapshot(
        &self,
        phase: &str,
        kind: ProgressKind,
        now: Instant,
        event_limit_reached: bool,
    ) -> ProgressSnapshot {
        let (phase, detail_truncated) = truncate_utf8(phase, self.max_detail_bytes);
        ProgressSnapshot {
            phase,
            kind,
            elapsed: now.duration_since(self.started),
            detail_truncated,
            event_limit_reached,
        }
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_owned(), false);
    }
    let mut boundary = max_bytes.min(value.len());
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    (value[..boundary].to_owned(), true)
}

/// Writes progress to stderr, throttled to 1 update per second.
pub struct StderrReporter {
    last_report: Mutex<Instant>,
}

impl Default for StderrReporter {
    fn default() -> Self {
        Self::new()
    }
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
impl Default for VecReporter {
    fn default() -> Self {
        Self::new()
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

    #[test]
    fn machine_progress_coalesces_periodic_updates() {
        let mut progress = BoundedProgress::default();
        let first = progress.periodic("executing").expect("first snapshot");
        assert_eq!(first.kind(), ProgressKind::Periodic);
        assert!(progress.periodic("executing").is_none());
    }

    #[test]
    fn machine_progress_bounds_utf8_detail_and_discardable_events() {
        let mut progress = BoundedProgress::new(1, 5);
        let transition = progress.transition("éééé");
        assert_eq!(transition.phase(), "éé");
        assert!(transition.detail_truncated());
        assert!(!transition.event_limit_reached());

        assert!(progress.periodic_at("work", Instant::now()).is_some());
        let later = Instant::now() + Duration::from_secs(2);
        let capped = progress
            .periodic_at("work", later)
            .expect("one cap notification");
        assert!(capped.event_limit_reached());
        assert!(
            progress
                .periodic_at("ignored", later + Duration::from_secs(2))
                .is_none()
        );

        let finalizing = progress.transition("finalizing");
        assert_eq!(finalizing.kind(), ProgressKind::Transition);
        assert!(!finalizing.event_limit_reached());
    }
}
