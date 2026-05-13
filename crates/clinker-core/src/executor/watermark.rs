//! Per-(source, file) event-time watermark bookkeeping.
//!
//! [`ExecutorContext`] owns one [`PerSourceWatermarks`] for the
//! pipeline run. State is keyed by `(source_name, Arc<source_file>)` —
//! the same `$source.file` Arc that travels on every record's
//! engine-stamped lineage column — so a glob/regex/paths source that
//! pulls N files keeps N independent watermarks. Collapsing N files
//! into one watermark would cross-gate file A's windows against file
//! B's record stream, breaking the 1:1 source-file → sink invariant
//! that `fan_out_per_source_file` outputs rely on.
//!
//! Sources declare their event-time column via `watermark:` on
//! [`crate::config::SourceConfig`] (column applies to every file the
//! source pulls in). `ingest_source_into_stream` observes per record
//! using the reader's `current_source_file()` as the file key and
//! folds the column value into an i64-nanos max for that pair.
//!
//! Read-side rollup methods serve every consumer granularity:
//! - [`source_min`](PerSourceWatermarks::source_min) — min across all
//!   files of one source (per-file watermarks compose at the source
//!   level via min, so a glob source with one lagging file holds the
//!   source-level watermark back, matching the Flink/Arroyo
//!   per-partition + operator-level reducer pattern).
//! - [`min_across_sources`](PerSourceWatermarks::min_across_sources) —
//!   min across rolled-up source values; the cross-source operator-
//!   level reducer a time-windowed close decision reads.
//!
//! `None` semantics at every level mirror Spark Structured Streaming:
//! a partition / source with no observations contributes `None` and
//! is excluded from the min reducer. A min reducer over an all-`None`
//! input returns `None`. Idle-source flagging (Flink's
//! `WatermarkStatus.IDLE`) is deferred to the async migration sprint
//! where concurrent ingest can leave a source genuinely quiet while
//! peers advance; see https://github.com/rustpunk/clinker/issues/57.
//!
//! Reset on `$ck` rollback (https://github.com/rustpunk/clinker/issues/56)
//! is not wired here — pipeline-wide rollback rebuilds
//! [`ExecutorContext`] from scratch, so monotonicity within a run is
//! preserved by construction. When per-source rollback lands, the
//! reset method ships in that sprint alongside its caller — the rule
//! the project applies sprint-wide is that every `pub(crate)` API has
//! an intra-crate caller at the closing commit.
//!
//! [`ExecutorContext`]: crate::executor::dispatch::ExecutorContext

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Max event-time observed for one (source, file) partition, in
/// nanoseconds since the Unix epoch. `None` means the partition has
/// no observations — either zero records, or every record's
/// watermark column was Null / unparseable.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SourcePartitionWatermark {
    pub(crate) max_event_time_nanos: Option<i64>,
}

/// Per-source / per-file event-time watermark map.
///
/// Two tables kept in sync at the executor boundary:
///
/// - `declared`: set of source names that declared
///   [`crate::config::SourceConfig::watermark`]. The
///   [`ExecutionReport`] uses this to emit a per-source rollup entry
///   even when ingest observed zero records for that source.
/// - `by_source`: per-(source, file) max-event-time. Populated by
///   ingest [`observe`](Self::observe) calls. A source declared but
///   with zero observed records is absent from this table; rollups
///   treat the missing entry as `None`.
///
/// [`ExecutionReport`]: crate::executor::ExecutionReport
#[derive(Debug, Default)]
pub(crate) struct PerSourceWatermarks {
    declared: HashSet<String>,
    by_source: HashMap<String, HashMap<Arc<str>, SourcePartitionWatermark>>,
}

impl PerSourceWatermarks {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record that a source declared `watermark:` in its config.
    /// Idempotent. Called once per declared source at executor start,
    /// before any `observe` calls.
    pub(crate) fn declare(&mut self, source: &str) {
        self.declared.insert(source.to_string());
    }

    /// Fold one observation into the running max for `(source, file)`.
    /// The `Arc<str>` for the file is cloned only when a new partition
    /// entry materializes — the steady-state hot path reuses the
    /// caller's existing Arc identity (the same one that lands on the
    /// record's `$source.file` engine-stamped column).
    pub(crate) fn observe(&mut self, source: &str, file: &Arc<str>, ts_nanos: i64) {
        let partition = self
            .by_source
            .entry(source.to_string())
            .or_default()
            .entry(Arc::clone(file))
            .or_default();
        partition.max_event_time_nanos = Some(match partition.max_event_time_nanos {
            Some(prev) => prev.max(ts_nanos),
            None => ts_nanos,
        });
    }

    /// Min across all files of one source — the source-level rollup
    /// that holds back a glob source's effective watermark when one
    /// file lags. Returns `None` when the source has no populated
    /// partitions (no records observed yet, or no `watermark:`
    /// declared).
    pub(crate) fn source_min(&self, source: &str) -> Option<i64> {
        self.by_source
            .get(source)?
            .values()
            .filter_map(|p| p.max_event_time_nanos)
            .min()
    }

    /// Min across the rolled-up source-level values for the supplied
    /// names. The reducer a time-windowed aggregate's close decision
    /// reads (future consumer; today only [`ExecutionReport`] reads
    /// this). Sources whose [`source_min`](Self::source_min) is `None`
    /// are excluded from the min; an all-`None` input set returns
    /// `None`.
    ///
    /// [`ExecutionReport`]: crate::executor::ExecutionReport
    pub(crate) fn min_across_sources(&self, sources: &[&str]) -> Option<i64> {
        sources.iter().filter_map(|s| self.source_min(s)).min()
    }

    /// Iterate every populated (source, file, watermark) triple.
    /// Order is unspecified; the report layer collects into a
    /// [`std::collections::BTreeMap`] for deterministic snapshot
    /// ordering.
    pub(crate) fn iter_partitions(
        &self,
    ) -> impl Iterator<Item = (&str, &Arc<str>, &SourcePartitionWatermark)> + '_ {
        self.by_source
            .iter()
            .flat_map(|(src, files)| files.iter().map(move |(file, w)| (src.as_str(), file, w)))
    }

    /// Iterate every declared source name (regardless of whether it
    /// has any populated partitions yet). The report layer uses this
    /// to emit a per-source rollup entry for declared-but-zero-record
    /// sources.
    pub(crate) fn iter_declared_sources(&self) -> impl Iterator<Item = &str> + '_ {
        self.declared.iter().map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arc(s: &str) -> Arc<str> {
        Arc::from(s)
    }

    fn file_max_from_iter(w: &PerSourceWatermarks, src: &str, file: &str) -> Option<i64> {
        w.iter_partitions()
            .find(|(s, f, _)| *s == src && f.as_ref() == file)
            .and_then(|(_, _, p)| p.max_event_time_nanos)
    }

    #[test]
    fn observe_is_max_per_partition() {
        let mut w = PerSourceWatermarks::new();
        let a = arc("a.csv");
        w.observe("src", &a, 100);
        w.observe("src", &a, 50);
        w.observe("src", &a, 200);
        w.observe("src", &a, 150);
        assert_eq!(file_max_from_iter(&w, "src", "a.csv"), Some(200));
    }

    #[test]
    fn observe_keeps_files_independent() {
        let mut w = PerSourceWatermarks::new();
        let a = arc("a.csv");
        let b = arc("b.csv");
        w.observe("src", &a, 100);
        w.observe("src", &b, 999);
        assert_eq!(file_max_from_iter(&w, "src", "a.csv"), Some(100));
        assert_eq!(file_max_from_iter(&w, "src", "b.csv"), Some(999));
    }

    #[test]
    fn source_min_rolls_files_via_min() {
        let mut w = PerSourceWatermarks::new();
        let a = arc("a.csv");
        let b = arc("b.csv");
        w.observe("src", &a, 100);
        w.observe("src", &b, 999);
        // The lagging file holds the source-level watermark back —
        // file b's high mark cannot finalize file a's data
        // unilaterally.
        assert_eq!(w.source_min("src"), Some(100));
    }

    #[test]
    fn source_min_returns_none_for_unobserved_source() {
        let w = PerSourceWatermarks::new();
        assert_eq!(w.source_min("src"), None);
    }

    #[test]
    fn min_across_sources_skips_none_and_returns_min() {
        let mut w = PerSourceWatermarks::new();
        w.observe("src_a", &arc("a.csv"), 100);
        w.observe("src_b", &arc("b.csv"), 200);
        assert_eq!(w.min_across_sources(&["src_a", "src_b"]), Some(100));
        // An unobserved source is skipped, not treated as zero.
        assert_eq!(
            w.min_across_sources(&["src_a", "src_b", "src_unobs"]),
            Some(100),
        );
    }

    #[test]
    fn min_across_sources_returns_none_when_all_unobserved() {
        let w = PerSourceWatermarks::new();
        assert_eq!(w.min_across_sources(&["src_a", "src_b"]), None);
    }

    #[test]
    fn declare_alone_leaves_rollups_none() {
        let mut w = PerSourceWatermarks::new();
        w.declare("src");
        // Declaration alone does not populate by_source; rollups
        // still return None until `observe` runs.
        assert_eq!(w.source_min("src"), None);
        assert_eq!(file_max_from_iter(&w, "src", "any.csv"), None);
        let declared: Vec<&str> = w.iter_declared_sources().collect();
        assert_eq!(declared, vec!["src"]);
    }

    #[test]
    fn iter_declared_sources_lists_zero_record_sources() {
        let mut w = PerSourceWatermarks::new();
        w.declare("src_a");
        w.declare("src_b");
        let mut declared: Vec<&str> = w.iter_declared_sources().collect();
        declared.sort();
        assert_eq!(declared, vec!["src_a", "src_b"]);
    }

    #[test]
    fn iter_partitions_yields_every_populated_pair() {
        let mut w = PerSourceWatermarks::new();
        w.observe("src_a", &arc("a.csv"), 100);
        w.observe("src_a", &arc("b.csv"), 200);
        w.observe("src_b", &arc("c.csv"), 300);
        let mut triples: Vec<(String, String, Option<i64>)> = w
            .iter_partitions()
            .map(|(s, f, w)| {
                (
                    s.to_string(),
                    f.as_ref().to_string(),
                    w.max_event_time_nanos,
                )
            })
            .collect();
        triples.sort();
        assert_eq!(
            triples,
            vec![
                ("src_a".to_string(), "a.csv".to_string(), Some(100)),
                ("src_a".to_string(), "b.csv".to_string(), Some(200)),
                ("src_b".to_string(), "c.csv".to_string(), Some(300)),
            ],
        );
    }

    #[test]
    fn observe_reuses_existing_arc_for_repeat_files() {
        // The steady-state hot path (every record from the same file
        // through the same source) must not clone the Arc on each
        // observation — it should reuse the Arc identity recorded on
        // first contact.
        let mut w = PerSourceWatermarks::new();
        let file = arc("a.csv");
        let initial_strong = Arc::strong_count(&file);
        for ts in 0..1000 {
            w.observe("src", &file, ts);
        }
        // One additional strong reference: the one stored in the map.
        // 1000 observations did not 1000-x the count.
        assert_eq!(Arc::strong_count(&file), initial_strong + 1);
    }
}
