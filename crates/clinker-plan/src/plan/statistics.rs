//! Planner-wide column-statistics catalog.
//!
//! A single statistics facility any node can consult, populated across two
//! planes that never feed back into each other:
//!
//! * **Plane A — plan-time, metadata-derived.** Row counts derived from a
//!   source file's byte length divided by an average-record-bytes
//!   constant. This is the only statistic available before any record is
//!   read; it seeds the planner's combine-strategy and partition-bit
//!   decisions and is the figure `--explain` reports. It reuses the same
//!   `std::fs` metadata read that drives the per-node byte-volume
//!   estimates, never a second data-reading pass.
//!
//! * **Plane B — exec-time, sketch-maintained.** Distinct counts
//!   (HyperLogLog), heavy hitters (Misra-Gries), and membership (Bloom)
//!   accumulated by [`RuntimeStatistics`] as records flow during a run.
//!   These are read back by the same run's reporting and by downstream
//!   in-DAG nodes that execute *after* the producer. They never re-decide
//!   an upstream plan node: a record-derived statistic cannot exist when
//!   that upstream node was already planned.
//!
//! Every column-level field is `Option`, so a stat that was never
//! gathered renders as an honest absence rather than a fabricated zero —
//! the same null floor the combine `--explain` surface already keeps.
//!
//! ## Keying
//!
//! Column statistics key on [`StatKey`] — the upstream node's
//! author-visible **name** plus the column name, both `Arc<str>`. Row
//! counts key on the node name alone. Name-keying is load-bearing and
//! matches every surveyed ETL tool's side-table convention: a graph-layer
//! `NodeIndex` key would reintroduce the swap-remove staleness bug class
//! that `petgraph` node removal triggers, and the catalog is populated
//! before the execution DAG's `NodeIndex` space even exists.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::Value;

/// Average on-disk bytes per record, used to convert a source file's byte
/// length into a Plane A row-count estimate.
///
/// Underestimating biases the planner toward in-memory strategies;
/// overestimating biases toward disk-spilling ones. 1 KiB matches the
/// production-record size observed on enrichment pipelines and is the
/// single divisor the whole engine shares so every byte→row conversion
/// agrees — the grace-hash fire/partition heuristics read the catalog's
/// row count rather than re-deriving their own.
pub const RECORD_BYTES_ESTIMATE: u64 = 1024;

/// Convert a source's on-disk byte length into a Plane A row-count
/// estimate. Returns `None` for an unknown (`None`) or zero seed so the
/// caller records an honest absence rather than a zero-row claim.
pub fn rows_from_bytes(seed_bytes: Option<u64>) -> Option<u64> {
    match seed_bytes {
        Some(b) if b > 0 => Some((b / RECORD_BYTES_ESTIMATE).max(1)),
        _ => None,
    }
}

/// Where a statistic came from. Carried alongside every figure so
/// `--explain` can tell the reader whether a number is a metadata-derived
/// estimate or a record-exact sketch result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatSource {
    /// Derived from a source file's byte length (Plane A).
    FileMetadata,
    /// Supplied by an author-declared schema hint (Plane A).
    SchemaHint,
    /// Measured by an exec-time sketch while records flowed (Plane B).
    ExecSketch,
}

impl StatSource {
    /// Short human label for `--explain` rendering.
    pub fn label(self) -> &'static str {
        match self {
            StatSource::FileMetadata => "file metadata",
            StatSource::SchemaHint => "schema hint",
            StatSource::ExecSketch => "exec sketch",
        }
    }
}

/// A row-count figure paired with its provenance.
#[derive(Debug, Clone, Copy)]
pub struct RowCount {
    pub rows: u64,
    pub source: StatSource,
}

/// Finalized membership-sketch summary: the filter's bit width and probe
/// count plus whether the element count it was sized from was itself an
/// estimate (an HLL distinct-count) rather than an exact row count.
#[derive(Debug, Clone, Copy)]
pub struct BloomSummary {
    /// Filter width in bits.
    pub bit_count: u64,
    /// Hash probes per key.
    pub hash_count: u32,
    /// True when the sizing element count was an estimate, not exact.
    pub sized_from_estimate: bool,
}

/// Finalized per-column statistics. Every field is `Option`: a column the
/// run never sketched carries `None` rather than a fabricated figure.
#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    /// Distinct-value estimate and its provenance.
    pub distinct: Option<(u64, StatSource)>,
    /// Heavy hitters as `(value, lower-bound frequency)` pairs, descending
    /// by frequency. The frequency is a Misra-Gries lower bound — a value
    /// absent from this list may still be frequent, so the list only ever
    /// promotes a key, never excludes one.
    pub heavy_hitters: Option<Vec<(Value, u64)>>,
    /// Membership-sketch summary, when a Bloom filter was built.
    pub bloom: Option<BloomSummary>,
}

/// Plan-time-readable statistics catalog.
///
/// Holds already-finalized summaries: per-node row counts (Plane A seed,
/// optionally overwritten by an exec-time finalize) and per-column
/// [`ColumnStats`]. Plain owned data with no locks, so it clones with
/// `CompileArtifacts` and is read freely across the planner. Exec-time
/// accumulation happens in [`RuntimeStatistics`], whose finalized results
/// fold back in via [`StatisticsCatalog::record_distinct`] and friends.
#[derive(Debug, Default, Clone)]
pub struct StatisticsCatalog {
    row_counts: HashMap<Arc<str>, RowCount>,
    columns: HashMap<StatKey, ColumnStats>,
}

/// Name-keyed column-statistics key: the upstream node name plus the
/// column name. Both `Arc<str>` so cloning a key is a refcount bump.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StatKey {
    pub node: Arc<str>,
    pub column: Arc<str>,
}

impl StatKey {
    /// Construct a key from node and column names.
    pub fn new(node: impl Into<Arc<str>>, column: impl Into<Arc<str>>) -> Self {
        Self {
            node: node.into(),
            column: column.into(),
        }
    }
}

impl StatisticsCatalog {
    /// An empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed a node's Plane A row count from its file-metadata byte seed.
    /// A `None` or zero seed records nothing, preserving the honest-null
    /// floor.
    pub fn seed_row_count_from_bytes(
        &mut self,
        node: impl Into<Arc<str>>,
        seed_bytes: Option<u64>,
    ) {
        if let Some(rows) = rows_from_bytes(seed_bytes) {
            self.row_counts.insert(
                node.into(),
                RowCount {
                    rows,
                    source: StatSource::FileMetadata,
                },
            );
        }
    }

    /// Record an exec-time-finalized exact row count for a node,
    /// overwriting any Plane A estimate with the measured figure.
    pub fn record_row_count(&mut self, node: impl Into<Arc<str>>, rows: u64) {
        self.row_counts.insert(
            node.into(),
            RowCount {
                rows,
                source: StatSource::ExecSketch,
            },
        );
    }

    /// Row count for a node, if known. The bare count — the planner's
    /// strategy heuristics read this.
    pub fn row_count(&self, node: &str) -> Option<u64> {
        self.row_counts.get(node).map(|rc| rc.rows)
    }

    /// Row count for a node with its provenance, for reporting.
    pub fn row_count_with_source(&self, node: &str) -> Option<RowCount> {
        self.row_counts.get(node).copied()
    }

    /// Record an exec-time distinct-count estimate for a column.
    pub fn record_distinct(&mut self, key: StatKey, distinct: u64) {
        self.columns
            .entry(key)
            .or_default()
            .distinct
            .replace((distinct, StatSource::ExecSketch));
    }

    /// Record an exec-time heavy-hitter list for a column.
    pub fn record_heavy_hitters(&mut self, key: StatKey, hitters: Vec<(Value, u64)>) {
        self.columns
            .entry(key)
            .or_default()
            .heavy_hitters
            .replace(hitters);
    }

    /// Record an exec-time membership-sketch summary for a column.
    pub fn record_bloom(&mut self, key: StatKey, summary: BloomSummary) {
        self.columns.entry(key).or_default().bloom.replace(summary);
    }

    /// Column statistics for a `(node, column)` pair, if any were
    /// recorded.
    pub fn column(&self, node: &str, column: &str) -> Option<&ColumnStats> {
        self.columns.get(&StatKey::new(node, column))
    }

    /// Iterate every node that carries a row count, with its figure and
    /// provenance, in node-name order for deterministic `--explain`.
    pub fn row_counts_sorted(&self) -> Vec<(&Arc<str>, RowCount)> {
        let mut entries: Vec<(&Arc<str>, RowCount)> =
            self.row_counts.iter().map(|(k, &v)| (k, v)).collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        entries
    }

    /// Iterate every column that carries statistics, in `(node, column)`
    /// order for deterministic `--explain`.
    pub fn columns_sorted(&self) -> Vec<(&StatKey, &ColumnStats)> {
        let mut entries: Vec<(&StatKey, &ColumnStats)> = self.columns.iter().collect();
        entries.sort_by(|a, b| {
            a.0.node
                .cmp(&b.0.node)
                .then_with(|| a.0.column.cmp(&b.0.column))
        });
        entries
    }

    /// True when the catalog holds no statistics at all — the signal
    /// `--explain` uses to skip its Statistics section entirely.
    pub fn is_empty(&self) -> bool {
        self.row_counts.is_empty() && self.columns.is_empty()
    }

    /// Fold every entry from an exec-time-finalized catalog into this one.
    /// Exec-measured figures overwrite Plane A estimates for the same key,
    /// since a record-exact result supersedes a metadata-derived guess.
    pub fn merge_from(&mut self, other: &StatisticsCatalog) {
        for (node, &rc) in &other.row_counts {
            self.row_counts.insert(Arc::clone(node), rc);
        }
        for (key, stats) in &other.columns {
            self.columns.insert(key.clone(), stats.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_from_bytes_floors_and_rejects_zero() {
        // Exactly the divisor → one row; below it but non-zero → still one
        // (a non-empty file holds at least one record); zero/unknown → none.
        assert_eq!(rows_from_bytes(Some(RECORD_BYTES_ESTIMATE)), Some(1));
        assert_eq!(rows_from_bytes(Some(RECORD_BYTES_ESTIMATE * 3)), Some(3));
        assert_eq!(rows_from_bytes(Some(1)), Some(1));
        assert_eq!(rows_from_bytes(Some(0)), None);
        assert_eq!(rows_from_bytes(None), None);
    }

    #[test]
    fn plane_a_seed_then_exec_overwrite_supersedes() {
        let mut cat = StatisticsCatalog::new();
        // Plane A: a 3 KiB file seeds ~3 rows tagged file-metadata.
        cat.seed_row_count_from_bytes("orders", Some(RECORD_BYTES_ESTIMATE * 3));
        assert_eq!(cat.row_count("orders"), Some(3));
        assert_eq!(
            cat.row_count_with_source("orders").map(|rc| rc.source),
            Some(StatSource::FileMetadata)
        );
        // Plane B: the exact post-read count supersedes the estimate and
        // re-tags the provenance as exec-measured.
        cat.record_row_count("orders", 2_999);
        let rc = cat.row_count_with_source("orders").unwrap();
        assert_eq!(rc.rows, 2_999);
        assert_eq!(rc.source, StatSource::ExecSketch);
    }

    #[test]
    fn unseeded_node_reports_honest_absence() {
        let cat = StatisticsCatalog::new();
        assert!(cat.is_empty());
        assert_eq!(cat.row_count("missing"), None);
        assert!(cat.column("missing", "id").is_none());
    }

    #[test]
    fn column_stats_record_and_read_back() {
        let mut cat = StatisticsCatalog::new();
        let key = StatKey::new("customers", "region");
        cat.record_distinct(key.clone(), 12_431);
        cat.record_heavy_hitters(
            key.clone(),
            vec![(Value::from("US"), 9_000), (Value::from("CA"), 1_200)],
        );
        cat.record_bloom(
            key.clone(),
            BloomSummary {
                bit_count: 96_000,
                hash_count: 7,
                sized_from_estimate: true,
            },
        );
        let stats = cat.column("customers", "region").unwrap();
        assert_eq!(stats.distinct, Some((12_431, StatSource::ExecSketch)));
        assert_eq!(stats.heavy_hitters.as_ref().unwrap().len(), 2);
        assert!(stats.bloom.unwrap().sized_from_estimate);
        assert!(!cat.is_empty());
    }

    #[test]
    fn merge_folds_exec_results_over_plane_a() {
        // A plan-time catalog seeded from metadata, and a runtime catalog
        // the executor accumulated into, fold into one with exec figures
        // winning on collision.
        let mut plan_time = StatisticsCatalog::new();
        plan_time.seed_row_count_from_bytes("orders", Some(RECORD_BYTES_ESTIMATE * 10));

        let mut runtime = StatisticsCatalog::new();
        runtime.record_row_count("orders", 9);
        runtime.record_distinct(StatKey::new("orders", "id"), 9);

        plan_time.merge_from(&runtime);
        // Exec-measured row count won over the metadata estimate.
        let rc = plan_time.row_count_with_source("orders").unwrap();
        assert_eq!(rc.rows, 9);
        assert_eq!(rc.source, StatSource::ExecSketch);
        // Column stats carried across the merge.
        assert_eq!(
            plan_time.column("orders", "id").and_then(|c| c.distinct),
            Some((9, StatSource::ExecSketch))
        );
    }

    #[test]
    fn row_counts_sorted_is_deterministic() {
        let mut cat = StatisticsCatalog::new();
        cat.record_row_count("zeta", 1);
        cat.record_row_count("alpha", 2);
        cat.record_row_count("mu", 3);
        let sorted = cat.row_counts_sorted();
        let names: Vec<&str> = sorted.iter().map(|(n, _)| n.as_ref()).collect();
        assert_eq!(names, ["alpha", "mu", "zeta"]);
    }
}
