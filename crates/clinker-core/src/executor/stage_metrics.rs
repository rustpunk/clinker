//! Per-stage pipeline instrumentation types.
//!
//! Provides timing, record counts, bytes written, RSS snapshots, and
//! optional heap-delta tracking (via the `bench-alloc` feature) for
//! each named pipeline stage.

use std::fmt;
use std::time::{Duration, Instant};

use crate::pipeline::memory::rss_bytes;

/// Metrics for a single pipeline stage.
#[derive(Debug, Clone)]
pub struct StageMetrics {
    pub name: StageName,
    pub elapsed: Duration,
    pub records_in: u64,
    pub records_out: u64,
    /// Write stage only — total bytes written (including across split rotations).
    pub bytes_written: Option<u64>,
    /// RSS snapshot at stage exit (always populated on supported platforms).
    pub rss_after: Option<u64>,
    /// Heap delta bytes over the stage's lifetime. `bench-alloc` feature-gated.
    pub heap_delta_bytes: Option<i64>,
    /// Number of heap allocations during the stage. `bench-alloc` feature-gated.
    pub heap_alloc_count: Option<u64>,
}

/// Named pipeline stages (not stringly-typed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageName {
    Compile,
    ReaderInit,
    SchemaScan,
    Sort,
    ArenaBuild,
    IndexBuild { name: String },
    TransformEval,
    Projection,
    RouteEval,
    Write,
    GroupFlush,
}

impl fmt::Display for StageName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StageName::Compile => write!(f, "Compile"),
            StageName::ReaderInit => write!(f, "ReaderInit"),
            StageName::SchemaScan => write!(f, "SchemaScan"),
            StageName::Sort => write!(f, "Sort"),
            StageName::ArenaBuild => write!(f, "ArenaBuild"),
            StageName::IndexBuild { name } => write!(f, "IndexBuild({name})"),
            StageName::TransformEval => write!(f, "TransformEval"),
            StageName::Projection => write!(f, "Projection"),
            StageName::RouteEval => write!(f, "RouteEval"),
            StageName::Write => write!(f, "Write"),
            StageName::GroupFlush => write!(f, "GroupFlush"),
        }
    }
}

/// Sequential stage timer with explicit `finish()`.
///
/// Captures `Instant::now()` on creation. Does NOT record on Drop —
/// sequential stages want completion semantics (no report on error).
/// On pipeline error the timer is simply dropped and no `ExecutionReport`
/// is generated anyway.
pub struct StageTimer {
    name: StageName,
    start: Instant,
    #[cfg(feature = "bench-alloc")]
    region: clinker_bench_support::alloc::Region,
}

impl StageTimer {
    pub fn new(name: StageName) -> Self {
        Self {
            name,
            start: Instant::now(),
            #[cfg(feature = "bench-alloc")]
            region: clinker_bench_support::alloc::Region::new(&clinker_bench_support::alloc::ALLOC),
        }
    }

    /// Finish the timer and return stage metrics.
    ///
    /// Captures RSS unconditionally (~1–5 µs on Linux, ~10–30 µs on macOS).
    /// At 6–10 calls per pipeline run the total overhead is under 50 µs.
    pub fn finish(self, records_in: u64, records_out: u64) -> StageMetrics {
        let elapsed = self.start.elapsed();
        let rss_after = rss_bytes();

        #[cfg(feature = "bench-alloc")]
        let (heap_delta, heap_count) = {
            let change = self.region.change();
            (Some(change.net_bytes()), Some(change.allocs as u64))
        };
        #[cfg(not(feature = "bench-alloc"))]
        let (heap_delta, heap_count) = (None, None);

        StageMetrics {
            name: self.name,
            elapsed,
            records_in,
            records_out,
            bytes_written: None,
            rss_after,
            heap_delta_bytes: heap_delta,
            heap_alloc_count: heap_count,
        }
    }
}

/// Cumulative timer for per-record sub-stages (Drop-based RAII).
///
/// DataFusion `ScopedTimerGuard` pattern. `guard()` returns a
/// [`TimerGuard`] that adds elapsed to the accumulator on Drop.
/// Correctly records partial work on `?` early returns (the sub-stage
/// ran for that duration regardless).
pub struct CumulativeTimer {
    nanos: u64,
}

impl Default for CumulativeTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl CumulativeTimer {
    pub fn new() -> Self {
        Self { nanos: 0 }
    }

    pub fn guard(&mut self) -> TimerGuard<'_> {
        TimerGuard {
            timer: self,
            start: Some(Instant::now()),
        }
    }

    /// Consume the timer and produce a [`StageMetrics`] entry.
    pub fn finish(self, name: StageName, records_in: u64, records_out: u64) -> StageMetrics {
        StageMetrics {
            name,
            elapsed: Duration::from_nanos(self.nanos),
            records_in,
            records_out,
            bytes_written: None,
            rss_after: None,
            heap_delta_bytes: None,
            heap_alloc_count: None,
        }
    }
}

/// RAII guard that adds elapsed to [`CumulativeTimer`] on drop.
///
/// `Option<Instant>` + `take()` pattern makes `stop()` idempotent —
/// calling `stop()` then dropping does not double-count.
pub struct TimerGuard<'a> {
    timer: &'a mut CumulativeTimer,
    start: Option<Instant>,
}

impl TimerGuard<'_> {
    pub fn stop(&mut self) {
        if let Some(start) = self.start.take() {
            self.timer.nanos += start.elapsed().as_nanos() as u64;
        }
    }
}

impl Drop for TimerGuard<'_> {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Accumulates per-stage metrics during DAG execution.
///
/// Passed as `&mut StageCollector` through `execute_dag` and all sub-paths.
/// Assembled into `ExecutionReport.stages` after execution completes.
/// Consistent with existing `&mut PipelineCounters` convention.
/// Do NOT use `mem::take` — push directly, consume with `into_stages()`.
#[derive(Debug, Default)]
pub struct StageCollector {
    stages: Vec<StageMetrics>,
}

impl StageCollector {
    pub fn record(&mut self, stage: StageMetrics) {
        self.stages.push(stage);
    }

    pub fn into_stages(self) -> Vec<StageMetrics> {
        self.stages
    }
}

/// Per-fold timing accumulator for rayon parallel chunk eval.
///
/// Used by two-pass parallel mode. Created per work-stealing task via
/// `fold()`, merged via `reduce()` after `par_iter` completes.
/// Zero contention — each fold task has its own `ChunkTimers`.
#[derive(Default, Clone)]
pub struct ChunkTimers {
    pub transform_eval: Duration,
    pub projection: Duration,
    pub write: Duration,
}

impl ChunkTimers {
    pub fn merge(mut self, other: Self) -> Self {
        self.transform_eval += other.transform_eval;
        self.projection += other.projection;
        self.write += other.write;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stage_timer_records_elapsed() {
        let timer = StageTimer::new(StageName::Compile);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let metrics = timer.finish(0, 0);
        assert!(metrics.elapsed >= Duration::from_millis(10));
    }

    #[test]
    fn test_stage_timer_finish_returns_correct_name() {
        let timer = StageTimer::new(StageName::ReaderInit);
        let metrics = timer.finish(5, 3);
        assert_eq!(metrics.name, StageName::ReaderInit);
        assert_eq!(metrics.records_in, 5);
        assert_eq!(metrics.records_out, 3);
    }

    #[test]
    fn test_stage_timer_finish_populates_rss_after() {
        let timer = StageTimer::new(StageName::Compile);
        let metrics = timer.finish(0, 0);
        // On Linux/macOS rss_bytes() returns Some; on unsupported platforms None
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
        assert!(metrics.rss_after.is_some());
    }

    #[test]
    fn test_cumulative_timer_accumulates_across_cycles() {
        let mut timer = CumulativeTimer::new();
        for _ in 0..3 {
            let _guard = timer.guard();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let metrics = timer.finish(StageName::TransformEval, 100, 100);
        assert!(metrics.elapsed >= Duration::from_millis(15));
    }

    #[test]
    fn test_cumulative_timer_guard_records_on_drop() {
        let mut timer = CumulativeTimer::new();
        {
            let _guard = timer.guard(); // no explicit stop()
            std::thread::sleep(std::time::Duration::from_millis(5));
        } // guard dropped here — should record
        let metrics = timer.finish(StageName::Write, 1, 1);
        assert!(metrics.elapsed >= Duration::from_millis(5));
    }

    #[test]
    fn test_cumulative_timer_stop_then_drop_no_double_count() {
        let mut timer = CumulativeTimer::new();
        {
            let mut guard = timer.guard();
            std::thread::sleep(std::time::Duration::from_millis(5));
            guard.stop(); // explicit stop
        } // drop after stop — should not double-count
        let metrics = timer.finish(StageName::Write, 1, 1);
        // Should be ~5ms, not ~10ms
        assert!(metrics.elapsed < Duration::from_millis(20));
    }

    #[test]
    fn test_cumulative_timer_zero_cycles_returns_zero() {
        let timer = CumulativeTimer::new();
        let metrics = timer.finish(StageName::Projection, 0, 0);
        assert_eq!(metrics.elapsed, Duration::ZERO);
    }

    #[test]
    fn test_stage_metrics_default_heap_fields_none() {
        let timer = StageTimer::new(StageName::Compile);
        let metrics = timer.finish(0, 0);
        // Without bench-alloc feature, heap fields are None
        #[cfg(not(feature = "bench-alloc"))]
        {
            assert!(metrics.heap_delta_bytes.is_none());
            assert!(metrics.heap_alloc_count.is_none());
        }
    }

    #[test]
    fn test_stage_metrics_default_bytes_written_none() {
        let timer = StageTimer::new(StageName::Write);
        let metrics = timer.finish(100, 100);
        assert!(metrics.bytes_written.is_none()); // Write stage sets this separately
    }

    #[test]
    fn test_stage_collector_record_and_into() {
        let mut collector = StageCollector::default();
        collector.record(StageTimer::new(StageName::Compile).finish(0, 0));
        collector.record(StageTimer::new(StageName::ReaderInit).finish(0, 0));
        collector.record(StageTimer::new(StageName::SchemaScan).finish(5, 1));
        let stages = collector.into_stages();
        assert_eq!(stages.len(), 3);
        assert_eq!(stages[0].name, StageName::Compile);
        assert_eq!(stages[2].name, StageName::SchemaScan);
    }

    #[test]
    fn test_stage_collector_empty_into() {
        let collector = StageCollector::default();
        assert!(collector.into_stages().is_empty());
    }

    #[test]
    fn test_chunk_timers_merge() {
        let a = ChunkTimers {
            transform_eval: Duration::from_millis(10),
            projection: Duration::from_millis(5),
            write: Duration::from_millis(8),
        };
        let b = ChunkTimers {
            transform_eval: Duration::from_millis(12),
            projection: Duration::from_millis(3),
            write: Duration::from_millis(7),
        };
        let merged = a.merge(b);
        assert_eq!(merged.transform_eval, Duration::from_millis(22));
        assert_eq!(merged.projection, Duration::from_millis(8));
        assert_eq!(merged.write, Duration::from_millis(15));
    }

    #[test]
    fn test_chunk_timers_merge_identity() {
        let a = ChunkTimers {
            transform_eval: Duration::from_millis(10),
            projection: Duration::from_millis(5),
            write: Duration::from_millis(8),
        };
        let merged = a.clone().merge(ChunkTimers::default());
        assert_eq!(merged.transform_eval, Duration::from_millis(10));
    }

    #[test]
    fn test_stage_name_index_build_equality() {
        let a = StageName::IndexBuild {
            name: "employees:dept".into(),
        };
        let b = StageName::IndexBuild {
            name: "employees:dept".into(),
        };
        let c = StageName::IndexBuild {
            name: "employees:region".into(),
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_stage_name_display() {
        assert_eq!(StageName::Compile.to_string(), "Compile");
        assert_eq!(StageName::ReaderInit.to_string(), "ReaderInit");
        assert_eq!(StageName::SchemaScan.to_string(), "SchemaScan");
        assert_eq!(StageName::Sort.to_string(), "Sort");
        assert_eq!(StageName::ArenaBuild.to_string(), "ArenaBuild");
        assert_eq!(
            StageName::IndexBuild {
                name: "employees:dept".into()
            }
            .to_string(),
            "IndexBuild(employees:dept)"
        );
        assert_eq!(StageName::TransformEval.to_string(), "TransformEval");
        assert_eq!(StageName::Projection.to_string(), "Projection");
        assert_eq!(StageName::RouteEval.to_string(), "RouteEval");
        assert_eq!(StageName::Write.to_string(), "Write");
        assert_eq!(StageName::GroupFlush.to_string(), "GroupFlush");
    }
}
