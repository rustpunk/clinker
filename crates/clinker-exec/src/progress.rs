//! Progress reporting for pipeline execution.
//!
//! One producer, sampled by observers. [`RunProgress`] is the shared counter
//! handle the executor advances while it runs; a reporter on another thread
//! samples it on its own clock and [`BoundedProgress`] coalesces those samples
//! into the bounded [`ProgressSnapshot`] records the machine stream carries.
//!
//! Counters are published, never pushed: the executor calls no reporter, so a
//! new rendering costs a reader and nothing on the hot path.
//!
//! Nothing here derives a ratio, and no consumer is handed one. A count and a
//! total travel as two plain numbers, so a renderer that wants a fraction
//! decides for itself what an absent total means rather than inheriting a
//! judgement made here.
//!
//! Record counts carry no denominator. A streaming source cannot bound its own
//! cardinality without reading it to the end, so the run has no honest record
//! total to divide by. The file axis carries the one denominator this engine
//! does establish ahead of the read: a source's file set is enumerated before
//! any of it is opened.
//!
//! The byte axis carries the denominator comparable engines actually report.
//! Bytes are counted inside the format layer's single byte-source funnel, so a
//! reader neither implements nor delegates anything to be counted. `bytes_total`
//! is withdrawn when a source's size cannot be read, and when any reader makes
//! more than one pass over its input — those bytes cross the counter twice, so
//! the count measures IO performed rather than input consumed and would overrun
//! the very total it was to be divided by.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

const DEFAULT_MAX_MACHINE_EVENTS: usize = 128;
const DEFAULT_MAX_DETAIL_BYTES: usize = 64;

/// Live run counters, alone on a cache line.
///
/// The alignment keeps a hot writer from sharing a line with whatever the
/// allocator places next: the padding is free while one thread writes, and a
/// cold neighbour on the same line is not.
#[repr(align(64))]
#[derive(Debug, Default)]
struct ProgressCounters {
    records_read: AtomicU64,
    files_done: AtomicU64,
}

/// Cloneable handle to one run's live progress counters.
///
/// Clones share the counters. The executor advances them from the dispatch
/// thread; an observer on another thread samples them on its own schedule and
/// may read a value already superseded. That staleness is the contract, not a
/// defect — nothing may decide on a sampled value. Decisions read the exact
/// state the executor owns.
///
/// Both totals are sealed at most once, before the first source begins
/// reading, and each distinguishes three states a reader must tell apart: not
/// yet established, established as unknowable for this run's sources, and
/// established. A total a reader has once seen never reverts to an absence.
#[derive(Clone, Debug, Default)]
pub struct RunProgress {
    counters: Arc<ProgressCounters>,
    files_total: Arc<OnceLock<Option<u64>>>,
    bytes_total: Arc<OnceLock<Option<u64>>>,
    /// Bytes counted by the format layer as sources hand them to readers. Not
    /// one of [`ProgressCounters`]: nothing here increments it, and the
    /// executor never writes it — the count happens where the bytes are.
    bytes: clinker_format::ByteTally,
}

impl RunProgress {
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish `delta` more source records read.
    ///
    /// Callers accumulate in a plain local and flush here at a boundary they
    /// already cross; one atomic per record would cost more than the count is
    /// worth on a record-at-a-time path.
    pub fn advance_records(&self, delta: u64) {
        if delta > 0 {
            self.counters
                .records_read
                .fetch_add(delta, Ordering::Relaxed);
        }
    }

    /// Publish `delta` more source files fully consumed.
    pub fn advance_files(&self, delta: u64) {
        if delta > 0 {
            self.counters.files_done.fetch_add(delta, Ordering::Relaxed);
        }
    }

    /// Record how many source files this run will read, or `None` where any
    /// source's input is not an enumerated file set.
    ///
    /// Sealing is once-only; a later call is ignored rather than overwriting,
    /// so a denominator cannot drift mid-run.
    pub fn seal_files_total(&self, total: Option<u64>) {
        let _ = self.files_total.set(total);
    }

    /// Record the run's total input size in bytes, or `None` where any source
    /// cannot establish one. Sealed once, like the file total.
    ///
    /// A total of zero is stored as an absence. Zero is not a denominator: a
    /// reader dividing by it gets `NaN` rather than a ratio, and a run with no
    /// bytes to read is not a run nought per cent through its input. Enforced
    /// here rather than at the call site so no caller can publish the division
    /// by forgetting to guard it.
    pub fn seal_bytes_total(&self, total: Option<u64>) {
        let _ = self.bytes_total.set(total.filter(|bytes| *bytes > 0));
    }

    /// The counter to attach to each source so its bytes are counted as they
    /// are read. Handed to the format layer, which owns the counting.
    pub fn byte_tally(&self) -> clinker_format::ByteTally {
        self.bytes.clone()
    }

    /// Read every counter and both totals.
    ///
    /// The reads are independent, so the sample is not a consistent cut: a
    /// file that closes between the `records_read` load and the `files_done`
    /// load contributes its closure and not the records that preceded it.
    /// Only a display consumes this, and a display tolerates a reading that
    /// was true a moment ago; a decision would not, and none reads it.
    pub fn sample(&self) -> ProgressSample {
        ProgressSample {
            records_read: self.counters.records_read.load(Ordering::Relaxed),
            files_done: self.counters.files_done.load(Ordering::Relaxed),
            files_total: self.files_total.get().copied().flatten(),
            bytes_read: self.bytes.read(),
            bytes_total: self.bytes_total.get().copied().flatten(),
        }
    }
}

/// One reading of a run's live counters.
///
/// `records_read` is deliberately denominator-free. `files_total` is `None`
/// both before it is established and when a source's input is not an
/// enumerated file set; a reader that needs to tell those apart is asking a
/// question no progress record answers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProgressSample {
    pub records_read: u64,
    pub files_done: u64,
    pub files_total: Option<u64>,
    pub bytes_read: u64,
    pub bytes_total: Option<u64>,
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
    sample: ProgressSample,
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

    /// The counter reading this record was built from.
    pub const fn sample(&self) -> ProgressSample {
        self.sample
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

    pub fn transition(&mut self, phase: &str, sample: ProgressSample) -> ProgressSnapshot {
        self.snapshot(
            phase,
            ProgressKind::Transition,
            sample,
            Instant::now(),
            false,
        )
    }

    pub fn periodic(&mut self, phase: &str, sample: ProgressSample) -> Option<ProgressSnapshot> {
        self.periodic_at(phase, sample, Instant::now())
    }

    /// Deterministic-time form used by focused cadence tests.
    pub fn periodic_at(
        &mut self,
        phase: &str,
        sample: ProgressSample,
        now: Instant,
    ) -> Option<ProgressSnapshot> {
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
            return Some(self.snapshot(phase, ProgressKind::Periodic, sample, now, true));
        }
        self.periodic_emitted = self.periodic_emitted.saturating_add(1);
        Some(self.snapshot(phase, ProgressKind::Periodic, sample, now, false))
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
        sample: ProgressSample,
        now: Instant,
        event_limit_reached: bool,
    ) -> ProgressSnapshot {
        let (phase, detail_truncated) = truncate_utf8(phase, self.max_detail_bytes);
        ProgressSnapshot {
            phase,
            kind,
            elapsed: now.duration_since(self.started),
            sample,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_publish_what_the_executor_advanced() {
        let progress = RunProgress::new();
        progress.advance_records(40);
        progress.advance_records(2);
        progress.advance_files(1);

        let sample = progress.sample();
        assert_eq!(sample.records_read, 42);
        assert_eq!(sample.files_done, 1);
    }

    #[test]
    fn clones_share_one_set_of_counters() {
        let progress = RunProgress::new();
        let observer = progress.clone();
        progress.advance_records(7);
        assert_eq!(observer.sample().records_read, 7);
    }

    #[test]
    fn an_unsealed_total_is_absent_not_zero() {
        let progress = RunProgress::new();
        assert_eq!(progress.sample().files_total, None);
    }

    #[test]
    fn a_total_sealed_as_unknowable_stays_absent() {
        let progress = RunProgress::new();
        progress.seal_files_total(None);
        assert_eq!(progress.sample().files_total, None);
    }

    #[test]
    fn a_sealed_total_cannot_drift() {
        let progress = RunProgress::new();
        progress.seal_files_total(Some(4));
        progress.seal_files_total(Some(9));
        assert_eq!(progress.sample().files_total, Some(4));
    }

    #[test]
    fn bytes_read_reflects_what_the_sources_handed_out() {
        use std::io::Read as _;
        let progress = RunProgress::new();
        assert_eq!(progress.sample().bytes_read, 0);

        let source = clinker_format::ReopenableSource::buffer(std::io::Cursor::new(
            b"thirteen bytes".to_vec(),
        ))
        .expect("buffer")
        .with_tally(progress.byte_tally());
        let mut sink = Vec::new();
        source
            .open()
            .expect("open")
            .read_to_end(&mut sink)
            .expect("read");

        assert_eq!(progress.sample().bytes_read, 14);
    }

    /// A reader that converts for a second pass will deliver its bytes twice,
    /// so the count stops being "input consumed" and the denominator has to go
    /// — otherwise a supervisor divides by a total the numerator will overrun.
    #[test]
    fn a_sealed_byte_total_cannot_drift() {
        let progress = RunProgress::new();
        progress.seal_bytes_total(Some(1024));
        progress.seal_bytes_total(Some(4096));
        progress.seal_bytes_total(None);
        assert_eq!(
            progress.sample().bytes_total,
            Some(1024),
            "a reader that has seen the total must never see it withdrawn"
        );
    }

    #[test]
    fn a_byte_total_sealed_as_unknowable_stays_absent() {
        let progress = RunProgress::new();
        progress.seal_bytes_total(None);
        assert_eq!(progress.sample().bytes_total, None);
    }

    /// Zero is not a denominator — dividing by it yields `NaN`, not `0%` — so a
    /// run with no bytes to read reports an absence rather than a total no
    /// consumer can use.
    #[test]
    fn a_zero_byte_total_is_reported_as_an_absence() {
        let progress = RunProgress::new();
        progress.seal_bytes_total(Some(0));
        assert_eq!(progress.sample().bytes_total, None);
    }

    #[test]
    fn a_snapshot_carries_the_sample_it_was_built_from() {
        let progress = RunProgress::new();
        progress.advance_records(9);
        progress.seal_files_total(Some(3));
        let mut bounded = BoundedProgress::default();

        let snapshot = bounded.transition("planning", progress.sample());
        assert_eq!(snapshot.sample().records_read, 9);
        assert_eq!(snapshot.sample().files_total, Some(3));
    }

    #[test]
    fn machine_progress_coalesces_periodic_updates() {
        let mut progress = BoundedProgress::default();
        let sample = ProgressSample::default();
        let first = progress
            .periodic("executing", sample)
            .expect("first snapshot");
        assert_eq!(first.kind(), ProgressKind::Periodic);
        assert!(progress.periodic("executing", sample).is_none());
    }

    #[test]
    fn machine_progress_bounds_utf8_detail_and_discardable_events() {
        let mut progress = BoundedProgress::new(1, 5);
        let sample = ProgressSample::default();
        let transition = progress.transition("éééé", sample);
        assert_eq!(transition.phase(), "éé");
        assert!(transition.detail_truncated());
        assert!(!transition.event_limit_reached());

        assert!(
            progress
                .periodic_at("work", sample, Instant::now())
                .is_some()
        );
        let later = Instant::now() + Duration::from_secs(2);
        let capped = progress
            .periodic_at("work", sample, later)
            .expect("one cap notification");
        assert!(capped.event_limit_reached());
        assert!(
            progress
                .periodic_at("ignored", sample, later + Duration::from_secs(2))
                .is_none()
        );

        let finalizing = progress.transition("finalizing", sample);
        assert_eq!(finalizing.kind(), ProgressKind::Transition);
        assert!(!finalizing.event_limit_reached());
    }

    /// The cap notice is the one record that explains the silence after it, so
    /// a run that reaches the real default must emit it exactly once. Driven
    /// through injected instants: the one-second floor is not overridable, so
    /// reaching 128 records on a wall clock would take over two minutes.
    #[test]
    fn the_default_cap_announces_itself_exactly_once() {
        let mut progress = BoundedProgress::default();
        let sample = ProgressSample::default();
        let start = Instant::now();
        let mut ordinary = 0_usize;
        let mut notices = 0_usize;

        // Two hundred due periodic slots against a 128-record cap.
        for tick in 0..200 {
            let now = start + Duration::from_secs(tick * 2);
            if let Some(snapshot) = progress.periodic_at("executing", sample, now) {
                if snapshot.event_limit_reached() {
                    notices += 1;
                } else {
                    ordinary += 1;
                }
            }
        }

        assert_eq!(ordinary, DEFAULT_MAX_MACHINE_EVENTS);
        assert_eq!(notices, 1, "the cap announces itself once and only once");
    }

    #[test]
    fn a_restored_cap_notice_is_offered_once_more_and_no_further() {
        let mut progress = BoundedProgress::new(0, DEFAULT_MAX_DETAIL_BYTES);
        let sample = ProgressSample::default();
        let start = Instant::now();

        let first = progress
            .periodic_at("executing", sample, start)
            .expect("cap notice");
        assert!(first.event_limit_reached());

        progress.restore_event_limit_notice();
        let second = progress
            .periodic_at("executing", sample, start + Duration::from_secs(2))
            .expect("re-offered cap notice");
        assert!(second.event_limit_reached());

        assert!(
            progress
                .periodic_at("executing", sample, start + Duration::from_secs(4))
                .is_none()
        );
    }
}
