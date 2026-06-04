//! Time-windowed aggregate execution helpers.
//!
//! The event-time window operator lives on
//! [`crate::config::AggregateConfig::time_window`]; see
//! <https://github.com/rustpunk/clinker/issues/61> for the design.
//! The dispatch arm at [`crate::executor::dispatch`] keeps its
//! existing positional-aggregate fast path and branches into the
//! helpers here when `time_window: Some(_)`.
//!
//! The helpers are deliberately stateless — they assign records to
//! window bounds, walk the DAG to find upstream Sources, and
//! resolve per-record event-time from the engine-stamped
//! `$source.event_time` column. The per-window aggregator state and
//! the late-record DLQ routing live at the dispatch site so the
//! existing [`crate::aggregation::AggregateStream`] machinery is
//! reused without duplication.
//!
//! Window mathematics matches Flink TVF / Spark Structured Streaming /
//! Beam windowing in three flavours:
//!
//! - [`tumbling_window`] — non-overlapping fixed-size windows. Each
//!   record lands in exactly one window.
//! - [`hopping_windows`] — overlapping fixed-size windows advanced by
//!   `slide`. Each record fans out to up to `ceil(size / slide)`
//!   windows.
//! - Session windows — per-key gap-bounded; the state machine lives at
//!   the dispatch site because it needs the group-by key.
//!
//! Close-readiness gating reads `min_across_sources` over the upstream
//! Source set; window `[w_start, w_end)` is "closed" iff
//! `min_across_sources >= w_end + allowed_lateness`. Records assigned
//! to an already-closed window route to the DLQ as
//! [`clinker_core_types::dlq::DlqErrorCategory::LateRecord`].

use std::collections::HashSet;

use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::config::pipeline_node::SOURCE_EVENT_TIME_COLUMN;
#[cfg(test)]
use crate::config::pipeline_node::TimeWindowSpec;
use crate::plan::execution::{ExecutionPlanDag, PlanNode};
use clinker_record::{Record, Value};

/// Per-window bounds in i64 nanoseconds since the Unix epoch. The
/// `start` end is inclusive and the `end` end is exclusive, matching
/// Flink TVF and Spark `window()` semantics: a record at exactly
/// `w_end` belongs to the NEXT window, not this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct WindowBounds {
    pub(crate) start: i64,
    pub(crate) end: i64,
}

/// Walk the DAG upstream from `aggregate_idx` and collect the names of
/// every reachable [`PlanNode::Source`]. Used by the time-windowed
/// aggregate arm to read `min_across_sources` over the relevant
/// per-source watermark partitions only — sister branches in a
/// branching DAG do not affect this aggregate's close decision.
pub(crate) fn upstream_source_names(
    dag: &ExecutionPlanDag,
    aggregate_idx: NodeIndex,
) -> Vec<String> {
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    let mut stack: Vec<NodeIndex> = vec![aggregate_idx];
    let mut sources: Vec<String> = Vec::new();
    while let Some(idx) = stack.pop() {
        if !visited.insert(idx) {
            continue;
        }
        if let PlanNode::Source { name, .. } = &dag.graph[idx] {
            sources.push(name.clone());
            continue;
        }
        for pred in dag.graph.neighbors_directed(idx, Direction::Incoming) {
            stack.push(pred);
        }
    }
    sources.sort();
    sources.dedup();
    sources
}

/// Read the delay-corrected event-time stamped at ingest into
/// [`SOURCE_EVENT_TIME_COLUMN`]. Returns `None` when the source
/// declared no watermark (the column carries `Value::Null`), when the
/// column is absent (synthetic records from upstream operators), or
/// when the value is not a `Value::Integer`. Time-windowed aggregates
/// treat `None` as "unassignable" — those records skip windowing.
pub(crate) fn record_event_time_nanos(record: &Record) -> Option<i64> {
    match record.get(SOURCE_EVENT_TIME_COLUMN)? {
        Value::Integer(ts) => Some(*ts),
        _ => None,
    }
}

/// Single window enclosing event-time `t` for a TUMBLING spec of size
/// `size_nanos`. Window starts are aligned to multiples of `size_nanos`
/// from the i64-nanos epoch — Flink TVF's `TUMBLE(t, INTERVAL 'size')`.
///
/// `size_nanos` must be `> 0`; callers validate at the dispatch arm.
pub(crate) fn tumbling_window(t: i64, size_nanos: i64) -> WindowBounds {
    let start = t.div_euclid(size_nanos) * size_nanos;
    WindowBounds {
        start,
        end: start.saturating_add(size_nanos),
    }
}

/// Every window enclosing event-time `t` for a HOPPING spec of size
/// `size_nanos` advanced by `slide_nanos`. Returns windows in
/// ascending order of start. `slide >= size` produces zero or one
/// window per record (some records may fall in gaps); `slide < size`
/// produces overlap up to `ceil(size / slide)` windows. Matches
/// Flink TVF `HOP(t, INTERVAL 'slide', INTERVAL 'size')`, Beam
/// `SlidingWindows.of(size).every(slide)`, Kafka Streams
/// `TimeWindows.ofSizeWithNoGrace(size).advanceBy(slide)`.
///
/// `size_nanos` and `slide_nanos` must be `> 0`.
pub(crate) fn hopping_windows(t: i64, size_nanos: i64, slide_nanos: i64) -> Vec<WindowBounds> {
    // A window with start `w` contains `t` iff `w <= t < w + size`,
    // i.e. `w` ranges over multiples of `slide` in `(t - size, t]`.
    // Lowest multiple of `slide` strictly greater than `t - size` is
    // `((t - size).div_euclid(slide) + 1) * slide`. Highest is
    // `t.div_euclid(slide) * slide`. If `slide > size`, the range can
    // be empty for some `t`.
    let lower = t.saturating_sub(size_nanos);
    let k_min = lower.div_euclid(slide_nanos).saturating_add(1);
    let k_max = t.div_euclid(slide_nanos);
    if k_min > k_max {
        return Vec::new();
    }
    let mut out: Vec<WindowBounds> = Vec::with_capacity((k_max - k_min + 1) as usize);
    for k in k_min..=k_max {
        let start = k.saturating_mul(slide_nanos);
        out.push(WindowBounds {
            start,
            end: start.saturating_add(size_nanos),
        });
    }
    out
}

/// Convert a [`std::time::Duration`] to `i64` nanoseconds, saturating
/// to `i64::MAX` for the (theoretical) 292-year ceiling. Used by the
/// dispatch arm to convert `TimeWindowSpec` fields to the integer
/// arithmetic the window helpers work in.
pub(crate) fn duration_to_nanos(d: std::time::Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

/// True iff a window with end `w_end` is closed at the supplied
/// watermark and operator-side `allowed_lateness`. A `None` watermark
/// (no upstream Source has observed any record yet, or every upstream
/// Source is in `WatermarkStatus::Idle` / `NoObservation`) keeps every
/// window open — the time-windowed aggregate's emission decision is
/// load-bearingly conservative.
pub(crate) fn window_is_closed(
    w_end: i64,
    allowed_lateness_nanos: i64,
    watermark: Option<i64>,
) -> bool {
    match watermark {
        Some(wm) => wm >= w_end.saturating_add(allowed_lateness_nanos),
        None => false,
    }
}

/// Per-key session windowing state used by the SESSION arm.
///
/// Each `(group_by_key)` accumulates a sequence of session instances:
/// each instance carries the event-times of its first and most-recent
/// record. A new record at event-time `t` extends the latest open
/// session if `t - last_event_time <= gap`, otherwise it starts a new
/// session. End-of-input finalizes every session whose
/// `last_event_time + gap + allowed_lateness <= watermark` (always
/// true in batch mode at the max-observed watermark).
///
/// The session ID a record is assigned to becomes the additional
/// synthetic group-by component the dispatch arm uses to bucket
/// records through the standard `AggregateStream` — preserving the
/// existing per-(group_by) accumulator machinery.
#[derive(Debug, Default, Clone)]
pub(crate) struct SessionInstance {
    pub(crate) start: i64,
    pub(crate) last_event_time: i64,
}

impl SessionInstance {
    pub(crate) fn end(&self, gap_nanos: i64) -> i64 {
        self.last_event_time.saturating_add(gap_nanos)
    }

    /// Extend this session with a new record at `t`. The session's
    /// `last_event_time` becomes `max(prev, t)` so out-of-order records
    /// within the same session still produce a single per-(key, session)
    /// emission. Returns the extended session for fluent chaining.
    pub(crate) fn extend(&mut self, t: i64) {
        if t > self.last_event_time {
            self.last_event_time = t;
        }
    }
}

/// Partition a single key's event-time-sorted records into session
/// instances. Records arrive pre-sorted by `t`; the walker greedily
/// extends the current session while consecutive records are within
/// `gap`, otherwise starts a new session. Returns the per-record
/// session index (0-based) plus the list of session instances.
///
/// Sessions in batch mode never merge after construction because the
/// input is fully sorted — the sliding-merge case described in Flink
/// docs only arises in true streaming when out-of-order arrivals
/// bridge previously-separate sessions. The pre-sort invariant here
/// matches that constraint.
pub(crate) fn partition_into_sessions(
    sorted_event_times: &[i64],
    gap_nanos: i64,
) -> (Vec<usize>, Vec<SessionInstance>) {
    let mut sessions: Vec<SessionInstance> = Vec::new();
    let mut assignments: Vec<usize> = Vec::with_capacity(sorted_event_times.len());
    for &t in sorted_event_times {
        match sessions.last_mut() {
            Some(s) if t.saturating_sub(s.last_event_time) <= gap_nanos => {
                s.extend(t);
                let idx = sessions.len() - 1;
                assignments.push(idx);
            }
            _ => {
                sessions.push(SessionInstance {
                    start: t,
                    last_event_time: t,
                });
                assignments.push(sessions.len() - 1);
            }
        }
    }
    (assignments, sessions)
}

/// True iff a session is closed at the supplied watermark and
/// `allowed_lateness`. Mirrors [`window_is_closed`] for tumbling /
/// hopping but reads the per-session `end` (last_event_time + gap).
pub(crate) fn session_is_closed(
    session: &SessionInstance,
    gap_nanos: i64,
    allowed_lateness_nanos: i64,
    watermark: Option<i64>,
) -> bool {
    match watermark {
        Some(wm) => {
            wm >= session
                .end(gap_nanos)
                .saturating_add(allowed_lateness_nanos)
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn nanos(secs: i64) -> i64 {
        secs.saturating_mul(1_000_000_000)
    }

    #[test]
    fn tumbling_aligns_on_size_boundaries() {
        let size = nanos(60); // 1m
        let w = tumbling_window(nanos(125), size);
        assert_eq!(w.start, nanos(120));
        assert_eq!(w.end, nanos(180));
    }

    #[test]
    fn tumbling_handles_exact_boundary() {
        let size = nanos(60);
        // Exact-end boundary belongs to the NEXT window.
        let w = tumbling_window(nanos(120), size);
        assert_eq!(w.start, nanos(120));
        assert_eq!(w.end, nanos(180));
    }

    #[test]
    fn tumbling_handles_negative_time() {
        let size = nanos(60);
        let w = tumbling_window(nanos(-1), size);
        // `-1 div_euclid 60s` = -1 → window [-60, 0).
        assert_eq!(w.start, nanos(-60));
        assert_eq!(w.end, nanos(0));
    }

    #[test]
    fn hopping_overlapping_windows_match_size_over_slide() {
        // size=10m, slide=5m → 2 windows per record at most.
        let size = nanos(600);
        let slide = nanos(300);
        let windows = hopping_windows(nanos(700), size, slide);
        // t=700; windows ending after 700: w_start at 300 ends 900,
        // w_start at 600 ends 1200. Both contain 700.
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].start, nanos(300));
        assert_eq!(windows[0].end, nanos(900));
        assert_eq!(windows[1].start, nanos(600));
        assert_eq!(windows[1].end, nanos(1200));
    }

    #[test]
    fn hopping_slide_greater_than_size_can_yield_empty() {
        // size=1m, slide=2m → window starts at 0, 120, 240 (multiples
        // of slide). Records in [0, 60) belong to window [0, 60);
        // records in [60, 120) fall in the gap between windows and
        // produce no assignments.
        let size = nanos(60);
        let slide = nanos(120);
        // Gap region: t=90 falls in zero windows.
        let windows = hopping_windows(nanos(90), size, slide);
        assert!(windows.is_empty());
        // t=120 is the inclusive start of the next window.
        let windows = hopping_windows(nanos(120), size, slide);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].start, nanos(120));
        assert_eq!(windows[0].end, nanos(180));
        // t=0 sits at the start of the first window.
        let windows = hopping_windows(nanos(0), size, slide);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].start, nanos(0));
        assert_eq!(windows[0].end, nanos(60));
    }

    #[test]
    fn window_is_closed_returns_false_when_watermark_none() {
        let closed = window_is_closed(nanos(60), 0, None);
        assert!(!closed, "None watermark must keep every window open");
    }

    #[test]
    fn window_is_closed_compares_against_allowed_lateness() {
        let allowed = nanos(30);
        // w_end = 60s, allowed = 30s → close threshold = 90s.
        assert!(!window_is_closed(nanos(60), allowed, Some(nanos(89))));
        assert!(window_is_closed(nanos(60), allowed, Some(nanos(90))));
        assert!(window_is_closed(nanos(60), allowed, Some(nanos(91))));
    }

    #[test]
    fn session_partition_walks_sorted_times() {
        // gap = 5s. Records at 0, 1, 4, 10 → session 0 = {0, 1, 4},
        // session 1 = {10}.
        let gap = nanos(5);
        let times = vec![nanos(0), nanos(1), nanos(4), nanos(10)];
        let (assignments, sessions) = partition_into_sessions(&times, gap);
        assert_eq!(assignments, vec![0, 0, 0, 1]);
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].start, nanos(0));
        assert_eq!(sessions[0].last_event_time, nanos(4));
        assert_eq!(sessions[1].start, nanos(10));
        assert_eq!(sessions[1].last_event_time, nanos(10));
    }

    #[test]
    fn session_partition_empty_input() {
        let (a, s) = partition_into_sessions(&[], 1);
        assert!(a.is_empty());
        assert!(s.is_empty());
    }

    #[test]
    fn session_is_closed_uses_gap_plus_allowed_lateness() {
        let s = SessionInstance {
            start: nanos(0),
            last_event_time: nanos(10),
        };
        let gap = nanos(5);
        let allowed = nanos(2);
        // session end = 10 + 5 = 15, close threshold = 15 + 2 = 17.
        assert!(!session_is_closed(&s, gap, allowed, Some(nanos(16))));
        assert!(session_is_closed(&s, gap, allowed, Some(nanos(17))));
    }

    #[test]
    fn duration_to_nanos_round_trips() {
        let d = Duration::from_millis(123);
        assert_eq!(duration_to_nanos(d), 123_000_000);
    }

    /// Resolve a TimeWindowSpec's duration fields to i64 nanoseconds.
    /// Exercised via the public helpers below for parsing
    /// completeness.
    #[test]
    fn time_window_spec_durations_round_trip_to_nanos() {
        let spec = TimeWindowSpec::Hopping {
            size: Duration::from_secs(60),
            slide: Duration::from_millis(500),
        };
        match spec {
            TimeWindowSpec::Hopping { size, slide } => {
                assert_eq!(duration_to_nanos(size), nanos(60));
                assert_eq!(duration_to_nanos(slide), 500_000_000);
            }
            _ => unreachable!(),
        }
    }
}
